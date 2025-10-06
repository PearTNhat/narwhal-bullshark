# Copyright(C) Facebook, Inc. and its affiliates.
from datetime import datetime
from dateutil import parser
from glob import glob
from logging import exception
from multiprocessing import Pool
from os.path import join, basename
from re import findall, search
from statistics import mean
from functools import partial
import traceback

from benchmark.utils import Print


class ParseError(Exception):
    pass


class LogParser:
    def __init__(self, clients, primaries, workers, faults=0):
        inputs = [clients, primaries, workers]
        assert all(isinstance(x, list) for x in inputs)
        assert all(isinstance(x, tuple) for y in inputs for x in y) # Expecting (filename, content)
        assert all(x for x in inputs)

        self.faults = faults
        if isinstance(faults, int):
            self.committee_size = len(primaries) + int(faults)
            self.workers = len(workers) // len(primaries) if len(primaries) > 0 else 0
        else:
            self.committee_size = '?'
            self.workers = '?'

        # Parse the clients logs with error handling.
        try:
            with Pool() as p:
                results = p.map(self._parse_clients_safe, clients)
            # Filter out failed results
            results = [r for r in results if r is not None]
            if not results:
                raise ParseError('No valid client logs could be parsed')
            
            self.size, self.rate, self.start, misses, self.sent_samples = zip(*results)
            self.misses = sum(misses)
        except (ValueError, IndexError, AttributeError) as e:
            exception(e)
            raise ParseError(f'Failed to parse clients\' logs: {e}')

        # Parse the primaries logs with error handling.
        try:
            with Pool() as p:
                results = p.map(self._parse_primaries_safe, primaries)
            results = [r for r in results if r is not None]
            if not results:
                self.proposals, self.commits, self.configs, primary_ips = {}, {}, [{}], []
                Print.warn('No valid primary logs could be parsed. Performance results will be incomplete.')
            else:
                proposals, commits, self.configs, primary_ips = zip(*results)
                self.proposals = self._merge_results([x.items() for x in proposals])
                self.commits = self._merge_results([x.items() for x in commits])
        except (ValueError, IndexError, AttributeError) as e:
            exception(e)
            raise ParseError(f'Failed to parse nodes\' logs: {e}')

        # Parse the workers logs with error handling.
        try:
            with Pool() as p:
                results = p.map(self._parse_workers_safe, workers)
            results = [r for r in results if r is not None]
            if not results:
                self.sizes, self.received_samples, workers_ips = {}, [], []
                Print.warn('No valid worker logs could be parsed. Performance results will be incomplete.')
            else:
                sizes, self.received_samples, workers_ips = zip(*results)
                self.sizes = {
                    k: v for x in sizes for k, v in x.items() if k in self.commits
                }
        except (ValueError, IndexError, AttributeError) as e:
            exception(e)
            raise ParseError(f'Failed to parse workers\' logs: {e}')

        self.collocate = set(primary_ips) == set(workers_ips) if primary_ips and workers_ips else False

        if self.misses != 0:
            Print.warn(
                f'Clients missed their target rate {self.misses:,} time(s)'
            )

    def _parse_clients_safe(self, log_data):
        filename, content = log_data
        try:
            return self._parse_clients(content)
        except Exception as e:
            Print.warn(f'Failed to parse client log [{basename(filename)}]. Error: {e}')
            print(traceback.format_exc())
            return None

    def _parse_primaries_safe(self, log_data):
        filename, content = log_data
        try:
            return self._parse_primaries(content)
        except Exception as e:
            Print.warn(f'Failed to parse primary log [{basename(filename)}]. Error: {e}')
            print(traceback.format_exc())
            return None

    def _parse_workers_safe(self, log_data):
        filename, content = log_data
        try:
            return self._parse_workers(content)
        except Exception as e:
            Print.warn(f'Failed to parse worker log [{basename(filename)}]. Error: {e}')
            print(traceback.format_exc())
            return None

    def _merge_results(self, input):
        merged = {}
        for x in input:
            for k, v in x:
                if not isinstance(v, (int, float)):
                    continue
                if k not in merged or merged[k] > v:
                    merged[k] = v
        return merged

    def _parse_clients(self, log):
        size_match = search(r'Transactions size: (\d+)', log)
        if not size_match: raise ParseError('Transaction size not found')
        size = int(size_match.group(1))

        rate_match = search(r'Transactions rate: (\d+)', log)
        if not rate_match: raise ParseError('Transaction rate not found')
        rate = int(rate_match.group(1))

        start_match = search(r'(.*?) .* Start ', log)
        if not start_match: raise ParseError('Start timestamp not found')
        start = self._to_posix(start_match.group(1))

        misses = len(findall(r'rate too high', log))
        samples = {int(s): self._to_posix(t) for t, s in findall(r'(.*?) .* sample transaction (\d+)', log)}
        return size, rate, start, misses, samples

    def _parse_primaries(self, log):
        proposals = {d: self._to_posix(t) for t, d in findall(r'(.*?) .* Created B\d+\([^ ]+\) -> ([^ ]+=)', log)}
        commits = {d: self._to_posix(t) for t, d in findall(r'(.*?) .* Committed B\d+\([^ ]+\) -> ([^ ]+=)', log)}
        
        ip_match = search(r'booted on (/ip4/\d+.\d+.\d+.\d+)', log)
        if not ip_match: raise ParseError('IP address not found')
        ip = ip_match.group(1)

        def safe_int_search(p, l, default=0): return int(m.group(1)) if (m := search(p, l)) else default
        configs = {
            'header_size': safe_int_search(r'Header size .* (\d+)', log),
            'max_header_delay': safe_int_search(r'Max header delay .* (\d+)', log),
            'gc_depth': safe_int_search(r'Garbage collection depth .* (\d+)', log),
            'sync_retry_delay': safe_int_search(r'Sync retry delay .* (\d+)', log),
            'sync_retry_nodes': safe_int_search(r'Sync retry nodes .* (\d+)', log),
            'batch_size': safe_int_search(r'Batch size .* (\d+)', log),
            'max_batch_delay': safe_int_search(r'Max batch delay .* (\d+)', log),
            'max_concurrent_requests': safe_int_search(r'Max concurrent requests .* (\d+)', log)
        }
        return proposals, commits, configs, ip

    def _parse_workers(self, log):
        sizes = {d: int(s) for d, s in findall(r'Batch ([^ ]+) contains (\d+) B', log)}
        samples = {int(s): d for d, s in findall(r'Batch ([^ ]+) contains sample tx (\d+)', log)}
        
        ip_match = search(r'booted on (/ip4/\d+.\d+.\d+.\d+)', log)
        if not ip_match: raise ParseError('IP address not found')
        ip = ip_match.group(1)
        return sizes, samples, ip

    def _to_posix(self, string):
        return datetime.timestamp(parser.isoparse(string[:24]))

    def _consensus_throughput(self):
        if not self.commits or not self.proposals: return 0, 0, 0
        start, end = min(self.proposals.values()), max(self.commits.values())
        duration = end - start
        if duration <= 0: return 0, 0, 0
        bytes_sum = sum(self.sizes.values())
        bps = bytes_sum / duration
        tps = bps / self.size[0] if self.size and self.size[0] > 0 else 0
        return tps, bps, duration

    def _consensus_latency(self):
        latency = [c - self.proposals[d] for d, c in self.commits.items() if d in self.proposals]
        return mean(latency) if latency else 0

    def _end_to_end_throughput(self):
        if not self.commits or not self.start: return 0, 0, 0
        start, end = min(self.start), max(self.commits.values())
        duration = end - start
        if duration <= 0: return 0, 0, 0
        bytes_sum = sum(self.sizes.values())
        bps = bytes_sum / duration
        tps = bps / self.size[0] if self.size and self.size[0] > 0 else 0
        return tps, bps, duration

    def _end_to_end_latency(self):
        latency = []
        for sent, received in zip(self.sent_samples, self.received_samples):
            for tx_id, batch_id in received.items():
                if batch_id in self.commits and tx_id in sent:
                    latency.append(self.commits[batch_id] - sent[tx_id])
        return mean(latency) if latency else 0

    def result(self):
        config = self.configs[0] if self.configs else {}
        consensus_tps, consensus_bps, _ = self._consensus_throughput()
        end_to_end_tps, end_to_end_bps, duration = self._end_to_end_throughput()
        
        return (
            '\n'
            '-----------------------------------------\n'
            ' SUMMARY:\n'
            '-----------------------------------------\n'
            ' + CONFIG:\n'
            f' Faults: {self.faults} node(s)\n'
            f' Committee size: {self.committee_size} node(s)\n'
            f' Worker(s) per node: {self.workers} worker(s)\n'
            f' Collocate primary and workers: {self.collocate}\n'
            f' Input rate: {sum(self.rate):,} tx/s\n'
            f' Transaction size: {self.size[0] if self.size else 0:,} B\n'
            f' Execution time: {round(duration):,} s\n'
            '\n'
            f' Header size: {config.get("header_size", 0):,} B\n'
            f' Max header delay: {config.get("max_header_delay", 0):,} ms\n'
            f' GC depth: {config.get("gc_depth", 0):,} round(s)\n'
            f' Sync retry delay: {config.get("sync_retry_delay", 0):,} ms\n'
            f' Sync retry nodes: {config.get("sync_retry_nodes", 0):,} node(s)\n'
            f' batch size: {config.get("batch_size", 0):,} B\n'
            f' Max batch delay: {config.get("max_batch_delay", 0):,} ms\n'
            f' Max concurrent requests: {config.get("max_concurrent_requests", 0):,} \n'
            '\n'
            ' + RESULTS:\n'
            f' Consensus TPS: {round(consensus_tps):,} tx/s\n'
            f' Consensus BPS: {round(consensus_bps):,} B/s\n'
            f' Consensus latency: {round(self._consensus_latency() * 1_000):,} ms\n'
            '\n'
            f' End-to-end TPS: {round(end_to_end_tps):,} tx/s\n'
            f' End-to-end BPS: {round(end_to_end_bps):,} B/s\n'
            f' End-to-end latency: {round(self._end_to_end_latency() * 1_000):,} ms\n'
            '-----------------------------------------\n'
        )

    def print(self, filename):
        assert isinstance(filename, str)
        with open(filename, 'a') as f: f.write(self.result())

    @classmethod
    def process(cls, directory, faults=0):
        def read_file(filename):
            with open(filename, 'r') as f: return f.read()
        
        def read_logs(pattern):
            return [(f, read_file(f)) for f in sorted(glob(join(directory, pattern)))]

        clients = read_logs('client-*.log')
        primaries = read_logs('primary-*.log')
        workers = read_logs('worker-*.log')
        
        if not clients: raise ParseError('No client log files found')
        return cls(clients, primaries, workers, faults=faults)


class LogGrpcParser:
    def __init__(self, primaries, faults=0):
        # primaries is a list of (filename, content) tuples
        assert all(isinstance(x, tuple) for x in primaries)
        self.faults = faults

        with Pool() as p:
            results = p.map(self._parse_primaries_safe, primaries)
        self.grpc_ports = [r for r in results if r is not None]
        if not self.grpc_ports:
            raise ParseError('No valid gRPC ports could be parsed')

    def _parse_primaries_safe(self, log_data):
        filename, content = log_data
        try:
            return self._parse_primaries(content)
        except Exception as e:
            Print.warn(f'Failed to parse gRPC port from log [{basename(filename)}]. Error: {e}')
            print(traceback.format_exc())
            return None

    def _parse_primaries(self, log):
        port_match = search(r'Consensus API gRPC Server listening on /ip4/.+/tcp/(.+)/http', log)
        if not port_match: raise ParseError('gRPC port not found')
        return port_match.group(1)

    @classmethod
    def process(cls, directory, faults=0):
        def read_file(filename):
            with open(filename, 'r') as f: return f.read()
        
        primaries = [(f, read_file(f)) for f in sorted(glob(join(directory, 'primary-*.log')))]
        if not primaries: raise ParseError('No primary log files found')
        return cls(primaries, faults=faults)