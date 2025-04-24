from prefect import flow
from prefect.events import DeploymentEventTrigger

# Source for the code to deploy (here, a GitHub repo)
SOURCE_REPO="https://github.com/marty-bo/Prefect_test.git"

if __name__ == "__main__":
    flow.from_source(
        source=SOURCE_REPO,
        entrypoint="scripts/google_sheet_flows.py:write_status_in_sheet", # Specific flow to run
    ).deploy(
        name="google_sheet_deployment",
        parameters={
            "prob1": 0.5,
            "prob2": 0.8,
            "prob3": 0.5
        },
        work_pool_name="my-work-pool2",
        tags={"monitoring"},
        # cron="* */12 * * *",  # Run every 12 hours
    )

    flow.from_source(
        source=SOURCE_REPO,
        entrypoint="scripts/google_sheet_flows.py:analye_status"
    ).deploy(
        name="google_sheet_analyse_deployment",
        work_pool_name="my-work-pool2",
        tags={"monitoring"},
        triggers=[
            DeploymentEventTrigger(
                expect={"prefect.flow-run.Completed"},
                match_related={"prefect.resource.name": "google_sheet_deployment"}
            )
        ]
    )