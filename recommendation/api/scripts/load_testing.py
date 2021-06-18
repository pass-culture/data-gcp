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
async def scenario_a(session):
    time.sleep(SLEEP)
    async with session.get(f"{BASE_API_URL}/1?token={TOKEN}") as resp:
        assert resp.status == 200


@scenario(weight=100)
async def scenario_a_localized(session):
    time.sleep(SLEEP)
    async with session.get(
        f"{BASE_API_URL}/2?token={TOKEN}&longitude=2.0596113&latitude=48.8086136"
    ) as resp:
        assert resp.status == 200


@scenario(weight=0)
async def scenario_b(session):
    time.sleep(SLEEP)
    async with session.get(f"{BASE_API_URL}/2?token={TOKEN}") as resp:
        assert resp.status == 200


@scenario(weight=0)
async def scenario_b_localized(session):
    time.sleep(SLEEP)
    async with session.get(
        f"{BASE_API_URL}/2?token={TOKEN}&longitude=2.0596113&latitude=48.8086136"
    ) as resp:
        assert resp.status == 200


# On peut lancer la commande suivante :
# molotov load_testing.py -p 2 -w 2 -d 600
# on modifie les valeurs des arguments -p (processeurs), -w (workers), et -d (temps en secondes)
# de façon à obtenir le test souhaité. On peut également modifier le poids des scénarios et en écrire de nouveaux
