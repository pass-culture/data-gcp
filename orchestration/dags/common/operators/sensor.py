from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils import timezone
from airflow.utils.decorators import apply_defaults


class TimeSleepSensor(BaseSensorOperator):
    """
    Waits for specified time interval relative to task instance start

    :param sleep_duration: time after which the job succeeds
    :type sleep_duration: datetime.timedelta
    """

    @apply_defaults
    def __init__(self, execution_delay, sleep_duration, *args, **kwargs):
        super(TimeSleepSensor, self).__init__(*args, **kwargs)
        self.execution_delay = execution_delay
        self.sleep_duration = sleep_duration
        self.poke_interval = kwargs.get(
            "poke_interval", int(sleep_duration.total_seconds())
        )
        self.timeout = kwargs.get("timeout", int(sleep_duration.total_seconds()) + 30)

    def poke(self, context):
        ti = context["ti"]
        run_id = context["dag_run"].run_id
        is_scheduled = run_id.startswith("scheduled__")
        if is_scheduled:
            sensor_task_start_date = ti.execution_date + self.execution_delay
            target_time = sensor_task_start_date + self.sleep_duration
            target_result = timezone.utcnow() > target_time
            self.log.info(
                f"Task start: {sensor_task_start_date}, target time: {target_time}"
            )
            self.log.info(
                f"Checking time... {timezone.utcnow()}, Result : {target_result}"
            )

            return target_result
        else:
            self.log.info("Task not scheduled ! Pass.")
            return True
