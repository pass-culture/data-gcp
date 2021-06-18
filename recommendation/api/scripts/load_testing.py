import os
import time

from molotov import scenario

ENV = "stg"
BASE_API_URL = (
    "https://apireco-stg-yqkaec24jq-ew.a.run.app/recommendation/"
    if ENV == "stg"
    else "https://apireco-dev-yqkaec24jq-ew.a.run.app/recommendation/"
)
TOKEN = os.environ.get(f"API_TOKEN_{ENV}")
SLEEP = 0.5


@scenario(weight=0)
async def scenario_group_a_user(session):
    time.sleep(SLEEP)
    async with session.get(f"{BASE_API_URL}/1?token={TOKEN}") as resp:
        assert resp.status == 200


@scenario(weight=100)
async def scenario_group_a_user_localized(session):
    time.sleep(SLEEP)
    async with session.get(
        f"{BASE_API_URL}/2?token={TOKEN}&longitude=2.0596113&latitude=48.8086136"
    ) as resp:
        assert resp.status == 200


@scenario(weight=0)
async def scenario_group_b_user(session):
    time.sleep(SLEEP)
    async with session.get(f"{BASE_API_URL}/2?token={TOKEN}") as resp:
        assert resp.status == 200


@scenario(weight=0)
async def scenario_group_b_user_localized(session):
    time.sleep(SLEEP)
    async with session.get(
        f"{BASE_API_URL}/2?token={TOKEN}&longitude=2.0596113&latitude=48.8086136"
    ) as resp:
        assert resp.status == 200


# you can use the following command :
# molotov load_testing.py -p 2 -w 2 -d 600
# you may need to update these arguments -p (processors), -w (workers), et -d (time in seconds)
# to obtain the needed load test. You may also update the weight of each scenario and / or create new ones.
