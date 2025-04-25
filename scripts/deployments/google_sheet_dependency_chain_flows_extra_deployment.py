from prefect import flow
from prefect.events import DeploymentEventTrigger

# Source for the code to deploy (here, a GitHub repo)
SOURCE_REPO="https://github.com/marty-bo/Prefect_test.git"

if __name__ == "__main__":
    
    flow.from_source(
        source=SOURCE_REPO,
        entrypoint="scripts/google_sheet_dependency_chain_flows.py:task_1",
    ).deploy(
        name="dep_chain_task_1",
        parameters={},
        work_pool_name="my-work-pool2",
        tags={"dep_chain", "locking"},
        # cron="* */12 * * *",  # Run every 12 hours
    )

    for i in range(1, 6):
        flow.from_source(
            source=SOURCE_REPO,
            entrypoint=f"scripts/google_sheet_dependency_chain_flows.py:task_{i+1}",
        ).deploy(
            name=f"dep_chain_task_{i+1}",
            work_pool_name="my-work-pool2",
            tags={"dep_chain", "locking"},
            triggers=[
                DeploymentEventTrigger(
                    expect={"prefect.flow-run.Completed"},
                    match_related={"prefect.resource.name": f"dep_chain_task_{i}"}
                )
            ],
        )

        
