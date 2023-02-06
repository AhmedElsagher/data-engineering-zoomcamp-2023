from prefect import Flow
from prefect.storage import GitHub

flow = Flow(
    "gitlab-flow",
    GitHub(
        repo="https://github.com/AhmedElsagher/data-engineering-zoomcamp-2023/tree/week2_dev",                           # name of repo
        path="week2_dev/week_2_workflow_orchestration/flows/homework/etl_gcs_to_bg.py",                   # location of flow file in repo
        access_token_secret="$$$$$$$$$$$$$$$$$"  # name of personal access token secret
    )
)
