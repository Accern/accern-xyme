from typing import Any, List, IO, Dict, Optional, Iterable
import sys
import json
from concurrent.futures import Future

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic, ClusterMetadata


class KafkaConnect:
    def __init__(
            self,
            bootstrap: str = "localhost:9092,localhost:9101",
            group_prefix: str = "xyme-worker-",
            input_prefix: str = "xyme-input-",
            output_prefix: str = "xyme-output-",
            error_topic: str = "xyme-error") -> None:
        self._bootstrap = bootstrap
        self._group_prefix = group_prefix
        self._input_prefix = input_prefix
        self._output_prefix = output_prefix
        self._error_topic = error_topic
        self._consumers: Dict[Optional[str], Consumer] = {}
        self._producer: Optional[Producer] = None
        self._admin: Optional[AdminClient] = None

    def _get_input_topic(self, pipeline_id: str) -> str:
        return f"{self._input_prefix}{pipeline_id}"

    def _get_output_topic(self, pipeline_id: str) -> str:
        return f"{self._output_prefix}{pipeline_id}"

    def _get_error_topic(self) -> str:
        return self._error_topic

    def _get_consumer(
            self, pipe_id: Optional[str]) -> Consumer:
        res = self._consumers.get(pipe_id)
        if res is None:
            group_id = \
                f"{self._group_prefix}" \
                f"{'err' if pipe_id is None else pipe_id}"
            setting = {
                "bootstrap.servers": self._bootstrap,
                "group.id": group_id,
                # "auto.offset.reset": "earliest",  # FIXME: !!!!! configurable
            }
            consumer = Consumer(setting)
            if pipe_id is not None:
                consumer.subscribe([self._get_output_topic(pipe_id)])
            else:
                consumer.subscribe([self._get_error_topic()])
            self._consumers[pipe_id] = consumer
            res = consumer
        return res

    def _get_producer(self) -> Producer:
        res = self._producer
        if res is None:
            setting = {
                "bootstrap.servers": self._bootstrap,
            }
            producer = Producer(setting)
            self._producer = producer
            res = producer
        return res

    def _get_admin(self) -> AdminClient:
        res = self._admin
        if res is None:
            setting = {
                "bootstrap.servers": self._bootstrap,
            }
            admin = AdminClient(setting)
            self._admin = admin
            res = admin
        return res

    def close_all(self) -> None:
        consumers = list(self._consumers.values())
        self._consumers = {}
        for consumer in consumers:
            consumer.close()

    def create_topics(
            self,
            pipeline_id: str,
            num_partition: int,
            sync: bool = True) -> Dict[str, Future]:
        admin_client = self._get_admin()
        topic_list = [
            NewTopic(
                self._get_input_topic(pipeline_id),
                num_partitions=num_partition,
                replication_factor=1),
            NewTopic(
                self._get_output_topic(pipeline_id),
                num_partitions=1,
                replication_factor=1),
            NewTopic(
                self._get_error_topic(),
                num_partitions=1,
                replication_factor=1),
        ]
        res = admin_client.create_topics(topic_list)
        if sync:
            try:
                for val in res.values():
                    val.result()
            except KeyboardInterrupt:
                for val in res.values():
                    val.cancel()
        return res

    def delete_topics(
            self,
            pipeline_id: Optional[str],
            error_topic: bool,
            sync: bool = True) -> Dict[str, Future]:
        topics: List[str] = []
        if pipeline_id is not None:
            topics += [
                self._get_input_topic(pipeline_id),
                self._get_output_topic(pipeline_id),
            ]
        if error_topic:
            topics.append(self._get_error_topic())
        if not topics:
            return {}
        admin_client = self._get_admin()
        res = admin_client.delete_topics(topics)
        if sync:
            try:
                for val in res.values():
                    val.result()
            except KeyboardInterrupt:
                for val in res.values():
                    val.cancel()
        return res

    def push_data(
            self,
            pipeline_id: str,
            inputs: Iterable[bytes],
            stdout: Optional[IO[Any]] = sys.stdout) -> None:
        producer = self._get_producer()

        def delivery_report(err: Optional[str], msg: Any) -> None:
            if stdout is None:
                return
            out_msg = None
            if err is not None:
                out_msg = f"{err}"
            else:
                out_msg = f"kafka message: {msg.topic()} [{msg.partition()}]"
            if out_msg is not None:
                print(out_msg, file=stdout)

        input_topic = self._get_input_topic(pipeline_id)
        for item in inputs:
            producer.poll(0)
            producer.produce(input_topic, item, callback=delivery_report)
        producer.flush()

    def push_json(
            self,
            pipeline_id: str,
            inputs: Iterable[Any],
            stdout: Optional[IO[Any]] = sys.stdout) -> None:
        gen = (
            json.dumps(obj, indent=None, separators=(',', ':')).encode("utf-8")
            for obj in inputs
        )
        self.push_data(pipeline_id, gen, stdout)

    def read_output(
            self,
            pipeline_id: str,
            stderr: Optional[IO[Any]] = sys.stderr) -> Iterable[bytes]:
        consumer = self._get_consumer(pipeline_id)
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                break
            if msg.error():
                if stderr is not None:
                    print(f"{msg.error()}", file=stderr)
                continue
            yield msg.value()

    def read_json_output(
            self,
            pipeline_id: str,
            stderr: Optional[IO[Any]] = sys.stderr) -> Iterable[Any]:
        yield from (
            json.loads(item)
            for item in self.read_output(pipeline_id, stderr)
        )

    def read_error(
            self,
            stderr: Optional[IO[Any]] = sys.stderr) -> Iterable[str]:
        consumer = self._get_consumer(None)
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                break
            if msg.error():
                if stderr is not None:
                    print(f"{msg.error()}", file=stderr)
                continue
            yield msg.value().decose("utf-8")

    def list_topics(self) -> ClusterMetadata:
        consumer = self._get_consumer(None)
        return consumer.list_topics()
