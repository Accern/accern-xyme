from typing import Any, IO, Dict, Optional, Iterable
import sys
import json


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

    def _get_setting(
            self, pipeline_id: str, consumer: bool) -> Dict[str, str]:
        res = {
            "bootstrap.servers": self._bootstrap,
        }
        if consumer:
            res.update({
                "group.id": f"{self._group_prefix}{pipeline_id}",
                "auto.offset.reset": "earliest",  # FIXME: !!!!! configurable
            })
        return res

    def _get_input_topic(self, pipeline_id: str) -> str:
        return f"{self._input_prefix}{pipeline_id}"

    def _get_output_topic(self, pipeline_id: str) -> str:
        return f"{self._output_prefix}{pipeline_id}"

    def _get_error_topic(self) -> str:
        return self._error_topic

    def create_topics(self, pipeline_id: str) -> None:
        from confluent_kafka.admin import AdminClient, NewTopic

        setting = self._get_setting(pipeline_id, consumer=False)
        admin_client = AdminClient(setting)
        topic_list = [
            NewTopic(
                self._get_input_topic(pipeline_id),
                num_partitions=1,
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
        admin_client.create_topics(topic_list)

    def push_data(
            self,
            pipeline_id: str,
            inputs: Iterable[bytes],
            stdout: Optional[IO[Any]] = sys.stdout) -> None:
        from confluent_kafka import Producer

        setting = self._get_setting(pipeline_id, consumer=False)
        producer = Producer(setting)

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
        from confluent_kafka import Consumer

        setting = self._get_setting(pipeline_id, consumer=True)
        consumer = None
        try:
            consumer = Consumer(setting)
            consumer.subscribe([self._get_output_topic(pipeline_id)])
            while True:
                msg = consumer.poll(1.0)
                if msg is None:
                    break
                if msg.error():
                    if stderr is not None:
                        print(f"{msg.error()}", file=stderr)
                    continue
                yield msg.value()
        finally:
            if consumer is not None:
                consumer.close()

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
            pipeline_id: str,
            stderr: Optional[IO[Any]] = sys.stderr) -> Iterable[str]:
        from confluent_kafka import Consumer

        setting = self._get_setting(pipeline_id, consumer=True)
        consumer = None
        try:
            consumer = Consumer(setting)
            consumer.subscribe([self._get_error_topic()])
            while True:
                msg = consumer.poll(1.0)
                if msg is None:
                    break
                if msg.error():
                    if stderr is not None:
                        print(f"{msg.error()}", file=stderr)
                    continue
                yield msg.value().decose("utf-8")
        finally:
            if consumer is not None:
                consumer.close()
