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
    def __init__(self, sleep_duration, *args, **kwargs):
        super(TimeSleepSensor, self).__init__(*args, **kwargs)
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
            sensor_task_start_date = ti.start_date
            target_time = sensor_task_start_date + self.sleep_duration

            self.log.info(
                "Checking if the target time ({} - check:{}) has come - time to go: {}, start: {}, initial sleep_duration: {}".format(
                    target_time,
                    (timezone.utcnow() > target_time),
                    (target_time - timezone.utcnow()),
                    sensor_task_start_date,
                    self.sleep_duration,
                )
            )

            return timezone.utcnow() > target_time
        else:
            self.log.info("Task not scheduled ! Pass.")
            return True
