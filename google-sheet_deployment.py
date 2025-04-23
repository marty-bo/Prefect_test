from prefect import flow

# Source for the code to deploy (here, a GitHub repo)
SOURCE_REPO="https://github.com/marty-bo/Prefect_test.git"

if __name__ == "__main__":
    flow.from_source(
        source=SOURCE_REPO,
        entrypoint="google-sheet.py:write_status_in_sheet", # Specific flow to run
    ).deploy(
        name="google-sheet-deployment",
        parameters={
            "github_repos": [
                "PrefectHQ/prefect",
                "pydantic/pydantic",
                "huggingface/transformers"
            ]
        },
        work_pool_name="my-work-pool2",
        cron="0 * * * *",  # Run every hour
    )