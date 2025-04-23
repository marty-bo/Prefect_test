from importlib.metadata import entry_points
from prefect import flow
from prefect.events import DeploymentEventTrigger

# Source for the code to deploy (here, a GitHub repo)
SOURCE_REPO="https://github.com/marty-bo/Prefect_test.git"

if __name__ == "__main__":
    flow.from_source(
        source=SOURCE_REPO,
        entrypoint="google-sheet.py:write_status_in_sheet", # Specific flow to run
    ).deploy(
        name="google-sheet-deployment",
        parameters={
            "prob1": 0.5,
            "prob2": 0.2,
            "prob3": 0.05
        },
        work_pool_name="my-work-pool2",
        cron="*/5 * * * *",  # Run every 5 minutes
    )

    flow.from_source(
        source=SOURCE_REPO,
        entrypoint="google-sheet.py:analye_status"
    ).deploy(
        name="google-sheet-analyse-deployment",
        work_pool_name="my-work-pool2",
        triggers=[
            DeploymentEventTrigger(
                expect={"prefect.flow-run.Completed"},
                match_related={"prefect.resource.name": "google-sheet-deployment"}
            )
        ]
    )