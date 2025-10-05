# Copyright(C) Facebook, Inc. and its affiliates.
from datetime import datetime
from dateutil import parser
from glob import glob
from logging import exception
from multiprocessing import Pool
from os.path import join
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
        assert all(isinstance(x, str) for y in inputs for x in y)
        assert all(x for x in inputs)

        self.faults = faults
        if isinstance(faults, int):
            self.committee_size = len(primaries) + int(faults)
            self.workers = len(workers) // len(primaries)
        else:
            self.committee_size = '?'
            self.workers = '?'

        # Parse the clients logs with error handling
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

        # Parse the primaries logs with error handling
        try:
            with Pool() as p:
                results = p.map(self._parse_primaries_safe, primaries)
            # Filter out failed results
            results = [r for r in results if r is not None]
            if not results:
                raise ParseError('No valid primary logs could be parsed')
            
            proposals, commits, self.configs, primary_ips = zip(*results)
            self.proposals = self._merge_results([x.items() for x in proposals])
            self.commits = self._merge_results([x.items() for x in commits])
        except (ValueError, IndexError, AttributeError) as e:
            exception(e)
            raise ParseError(f'Failed to parse nodes\' logs: {e}')

        # Parse the workers logs with error handling
        try:
            with Pool() as p:
                results = p.map(self._parse_workers_safe, workers)
            # Filter out failed results
            results = [r for r in results if r is not None]
            if not results:
                raise ParseError('No valid worker logs could be parsed')
            
            sizes, self.received_samples, workers_ips = zip(*results)
            self.sizes = {
                k: v for x in sizes for k, v in x.items() if k in self.commits
            }
        except (ValueError, IndexError, AttributeError) as e:
            exception(e)
            raise ParseError(f'Failed to parse workers\' logs: {e}')

        # Determine whether the primary and the workers are collocated.
        self.collocate = set(primary_ips) == set(workers_ips)

        # Check whether clients missed their target rate.
        if self.misses != 0:
            Print.warn(
                f'Clients missed their target rate {self.misses:,} time(s)'
            )

    def _parse_clients_safe(self, log):
        """Wrapper with error handling for client parsing"""
        try:
            return self._parse_clients(log)
        except Exception as e:
            Print.warn(f'Failed to parse a client log: {e}')
            return None

    def _parse_primaries_safe(self, log):
        """Wrapper with error handling for primary parsing"""
        try:
            return self._parse_primaries(log)
        except Exception as e:
            Print.warn(f'Failed to parse a primary log: {e}')
            return None

    def _parse_workers_safe(self, log):
        """Wrapper with error handling for worker parsing"""
        try:
            return self._parse_workers(log)
        except Exception as e:
            Print.warn(f'Failed to parse a worker log: {e}')
            return None

    def _merge_results(self, input):
        # Keep the earliest timestamp.
        merged = {}
        for x in input:
            for k, v in x:
                if not isinstance(v, (int, float)):
                    continue
                if k not in merged or merged[k] > v:
                    merged[k] = v
        return merged

    def _parse_clients(self, log):
        if search(r'Error', log) is not None:
            raise ParseError('Client(s) panicked')

        size_match = search(r'Transactions size: (\d+)', log)
        if not size_match:
            raise ParseError('Transaction size not found in log')
        size = int(size_match.group(1))

        rate_match = search(r'Transactions rate: (\d+)', log)
        if not rate_match:
            raise ParseError('Transaction rate not found in log')
        rate = int(rate_match.group(1))

        start_match = search(r'(.*?) .* Start ', log)
        if not start_match:
            raise ParseError('Start timestamp not found in log')
        tmp = start_match.group(1)
        start = self._to_posix(tmp)

        misses = len(findall(r'rate too high', log))

        tmp = findall(r'(.*?) .* sample transaction (\d+)', log)
        samples = {}
        for t, s in tmp:
            try:
                samples[int(s)] = self._to_posix(t)
            except (ValueError, TypeError):
                continue

        return size, rate, start, misses, samples

    def _parse_primaries(self, log):
        if search(r'(?:panicked|ERROR)', log) is not None:
            raise ParseError('Primary(s) panicked')

        tmp = findall(r'(.*?) .* Created B\d+\([^ ]+\) -> ([^ ]+=)', log)
        proposals = {}
        for t, d in tmp:
            try:
                proposals[d] = self._to_posix(t)
            except (ValueError, TypeError):
                continue
        proposals = self._merge_results([proposals.items()])

        tmp = findall(r'(.*?) .* Committed B\d+\([^ ]+\) -> ([^ ]+=)', log)
        commits = {}
        for t, d in tmp:
            try:
                commits[d] = self._to_posix(t)
            except (ValueError, TypeError):
                continue
        commits = self._merge_results([commits.items()])

        def safe_int_search(pattern, log, default=0):
            match = search(pattern, log)
            if match:
                try:
                    return int(match.group(1))
                except (ValueError, TypeError):
                    return default
            return default

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

        ip_match = search(r'booted on (/ip4/\d+.\d+.\d+.\d+)', log)
        if not ip_match:
            raise ParseError('IP address not found in log')
        ip = ip_match.group(1)

        return proposals, commits, configs, ip

    def _parse_workers(self, log):
        if search(r'(?:panic|ERROR)', log) is not None:
            raise ParseError('Worker(s) panicked')

        tmp = findall(r'Batch ([^ ]+) contains (\d+) B', log)
        sizes = {}
        for d, s in tmp:
            try:
                sizes[d] = int(s)
            except (ValueError, TypeError):
                continue

        tmp = findall(r'Batch ([^ ]+) contains sample tx (\d+)', log)
        samples = {}
        for d, s in tmp:
            try:
                samples[int(s)] = d
            except (ValueError, TypeError):
                continue

        ip_match = search(r'booted on (/ip4/\d+.\d+.\d+.\d+)', log)
        if not ip_match:
            raise ParseError('IP address not found in log')
        ip = ip_match.group(1)

        return sizes, samples, ip

    def _to_posix(self, string):
        try:
            x = parser.isoparse(string[:24])
            return datetime.timestamp(x)
        except (ValueError, TypeError) as e:
            raise ParseError(f'Failed to parse timestamp: {string}')

    def _consensus_throughput(self):
        if not self.commits or not self.proposals:
            return 0, 0, 0
        
        try:
            start, end = min(self.proposals.values()), max(self.commits.values())
            duration = end - start
            
            if duration <= 0:
                Print.warn('Invalid duration for consensus throughput calculation')
                return 0, 0, 0
            
            bytes_sum = sum(self.sizes.values())
            bps = bytes_sum / duration
            
            if self.size[0] <= 0:
                Print.warn('Invalid transaction size')
                return 0, bps, duration
            
            tps = bps / self.size[0]
            return tps, bps, duration
        except (ValueError, ZeroDivisionError, TypeError) as e:
            Print.warn(f'Error calculating consensus throughput: {e}')
            return 0, 0, 0

    def _consensus_latency(self):
        try:
            latency = []
            for d, c in self.commits.items():
                if d in self.proposals:
                    lat = c - self.proposals[d]
                    if lat >= 0:  # Only include valid latencies
                        latency.append(lat)
            return mean(latency) if latency else 0
        except (ValueError, TypeError) as e:
            Print.warn(f'Error calculating consensus latency: {e}')
            return 0

    def _end_to_end_throughput(self):
        if not self.commits or not self.start:
            return 0, 0, 0
        
        try:
            start, end = min(self.start), max(self.commits.values())
            duration = end - start
            
            if duration <= 0:
                Print.warn('Invalid duration for end-to-end throughput calculation')
                return 0, 0, 0
            
            bytes_sum = sum(self.sizes.values())
            bps = bytes_sum / duration
            
            if self.size[0] <= 0:
                Print.warn('Invalid transaction size')
                return 0, bps, duration
            
            tps = bps / self.size[0]
            return tps, bps, duration
        except (ValueError, ZeroDivisionError, TypeError) as e:
            Print.warn(f'Error calculating end-to-end throughput: {e}')
            return 0, 0, 0

    def _end_to_end_latency(self):
        try:
            latency = []
            for sent, received in zip(self.sent_samples, self.received_samples):
                for tx_id, batch_id in received.items():
                    if batch_id in self.commits and tx_id in sent:
                        start = sent[tx_id]
                        end = self.commits[batch_id]
                        lat = end - start
                        if lat >= 0:  # Only include valid latencies
                            latency.append(lat)
            return mean(latency) if latency else 0
        except (ValueError, TypeError) as e:
            Print.warn(f'Error calculating end-to-end latency: {e}')
            return 0

    def result(self):
        header_size = self.configs[0]['header_size']
        max_header_delay = self.configs[0]['max_header_delay']
        gc_depth = self.configs[0]['gc_depth']
        sync_retry_delay = self.configs[0]['sync_retry_delay']
        sync_retry_nodes = self.configs[0]['sync_retry_nodes']
        batch_size = self.configs[0]['batch_size']
        max_batch_delay = self.configs[0]['max_batch_delay']
        max_concurrent_requests = self.configs[0]['max_concurrent_requests']

        consensus_latency = self._consensus_latency() * 1_000
        consensus_tps, consensus_bps, _ = self._consensus_throughput()
        end_to_end_tps, end_to_end_bps, duration = self._end_to_end_throughput()
        end_to_end_latency = self._end_to_end_latency() * 1_000

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
            f' Transaction size: {self.size[0]:,} B\n'
            f' Execution time: {round(duration):,} s\n'
            '\n'
            f' Header size: {header_size:,} B\n'
            f' Max header delay: {max_header_delay:,} ms\n'
            f' GC depth: {gc_depth:,} round(s)\n'
            f' Sync retry delay: {sync_retry_delay:,} ms\n'
            f' Sync retry nodes: {sync_retry_nodes:,} node(s)\n'
            f' batch size: {batch_size:,} B\n'
            f' Max batch delay: {max_batch_delay:,} ms\n'
            f' Max concurrent requests: {max_concurrent_requests:,} \n'
            '\n'
            ' + RESULTS:\n'
            f' Consensus TPS: {round(consensus_tps):,} tx/s\n'
            f' Consensus BPS: {round(consensus_bps):,} B/s\n'
            f' Consensus latency: {round(consensus_latency):,} ms\n'
            '\n'
            f' End-to-end TPS: {round(end_to_end_tps):,} tx/s\n'
            f' End-to-end BPS: {round(end_to_end_bps):,} B/s\n'
            f' End-to-end latency: {round(end_to_end_latency):,} ms\n'
            '-----------------------------------------\n'
        )

    def print(self, filename):
        assert isinstance(filename, str)
        try:
            with open(filename, 'a') as f:
                f.write(self.result())
        except IOError as e:
            Print.warn(f'Failed to write results to {filename}: {e}')

    @classmethod
    def process(cls, directory, faults=0):
        assert isinstance(directory, str)

        clients = []
        for filename in sorted(glob(join(directory, 'client-*.log'))):
            try:
                with open(filename, 'r') as f:
                    clients.append(f.read())
            except IOError as e:
                Print.warn(f'Failed to read {filename}: {e}')
        
        primaries = []
        for filename in sorted(glob(join(directory, 'primary-*.log'))):
            try:
                with open(filename, 'r') as f:
                    primaries.append(f.read())
            except IOError as e:
                Print.warn(f'Failed to read {filename}: {e}')
        
        workers = []
        for filename in sorted(glob(join(directory, 'worker-*.log'))):
            try:
                with open(filename, 'r') as f:
                    workers.append(f.read())
            except IOError as e:
                Print.warn(f'Failed to read {filename}: {e}')

        if not clients or not primaries or not workers:
            raise ParseError('Insufficient log files found')

        return cls(clients, primaries, workers, faults=faults)


class LogGrpcParser:
    def __init__(self, primaries, faults=0):
        assert all(isinstance(x, str) for x in primaries)
        self.faults = faults

        # Parse the primaries logs with error handling
        try:
            with Pool() as p:
                results = p.map(self._parse_primaries_safe, primaries)
            # Filter out failed results
            self.grpc_ports = [r for r in results if r is not None]
            if not self.grpc_ports:
                raise ParseError('No valid gRPC ports could be parsed')
        except (ValueError, IndexError, AttributeError) as e:
            exception(e)
            raise ParseError(f'Failed to parse nodes\' logs: {e}')

    def _parse_primaries_safe(self, log):
        """Wrapper with error handling for gRPC parsing"""
        try:
            return self._parse_primaries(log)
        except Exception as e:
            Print.warn(f'Failed to parse gRPC port from log: {e}')
            return None

    def _parse_primaries(self, log):
        port_match = search(
            r'Consensus API gRPC Server listening on /ip4/.+/tcp/(.+)/http', log)
        if not port_match:
            raise ParseError('gRPC port not found in log')
        return port_match.group(1)

    @classmethod
    def process(cls, directory, faults=0):
        assert isinstance(directory, str)

        primaries = []
        for filename in sorted(glob(join(directory, 'primary-*.log'))):
            try:
                with open(filename, 'r') as f:
                    primaries.append(f.read())
            except IOError as e:
                Print.warn(f'Failed to read {filename}: {e}')

        if not primaries:
            raise ParseError('No primary log files found')

        return cls(primaries, faults=faults)