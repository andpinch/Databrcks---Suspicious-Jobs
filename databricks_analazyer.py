from collections import defaultdict
from datetime import datetime, timedelta
import os
from typing import Dict, List, Mapping, Union
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import BaseRun
from dotenv import load_dotenv

class DatabricksAnalyzer:
    def __init__(self, api_endpoint, token):
        self.api_endpoint = api_endpoint
        self.token = token
        self.w = WorkspaceClient(host=self.api_endpoint, token=self.token)

    def get_all_job_runs(self, limit: int = 20) -> List:
        all_runs = []
        last_week_start = datetime.utcnow() - timedelta(weeks=2)

        response: list[BaseRun] = list(self.w.jobs.list_runs(
            limit=limit,
            start_time_from=int(last_week_start.timestamp() * 1000)
        ))

        all_runs.extend(response)

        while response and hasattr(response[-1], 'has_more') and response[-1].has_more:
            response = list(self.w.jobs.list_runs(
                limit=limit,
                page_token=response[-1].next_page_token,
                start_time_from=int(last_week_start.timestamp() * 1000)
            ))
            all_runs.extend(response)

        return all_runs

    def check_runs_exceeding_duration(self, all_runs: List, duration: timedelta) -> List:
        exceeded_duration_runs = [
            run for run in all_runs if self.calculate_runtime(
                datetime.utcfromtimestamp(run.start_time // 1000),
                datetime.utcfromtimestamp(run.end_time // 1000) if run.end_time else None
            ) > duration
        ]
        return exceeded_duration_runs

    def check_frequent_job_runs(self, all_runs: List, threshold: int, timeframe: timedelta) -> Mapping[str, List[Dict[str, Union[str, datetime, int]]]]:
        user_job_frequency = defaultdict(list)
        current_time = datetime.utcnow()

        for run in all_runs:
            start_time = datetime.utcfromtimestamp(run.start_time // 1000)
            end_time = datetime.utcfromtimestamp(run.end_time // 1000) if run.end_time else None
            runtime = self.calculate_runtime(start_time, end_time)

            if end_time and (current_time - end_time) <= timeframe:
                job_and_user = f"{run.job_id}_{run.creator_user_name}"
                user_job_frequency[job_and_user].append({
                    'run_id': run.run_id,
                    'start_time': start_time,
                    'end_time': end_time,
                    'runtime': runtime
                })

        frequent_job_runs = {
            key: runs for key, runs in user_job_frequency.items() if len(runs) > threshold
        }

        return frequent_job_runs

    def calculate_runtime(self, start_time, end_time) -> timedelta:
        if start_time and end_time:
            duration = end_time - start_time
            return duration
        else:
            return timedelta(seconds=0)

    def detect_high_frequency_jobs(self):
        all_runs = self.get_all_job_runs()
        run_duration = timedelta(hours=1)
        exceeded_duration = self.check_runs_exceeding_duration(all_runs, duration=run_duration)

        number_of_runs = 10
        timeframe = timedelta(hours=1)
        frequent_job_runs = self.check_frequent_job_runs(all_runs, threshold=number_of_runs, timeframe=timeframe)

        return {"exceeded_duration": exceeded_duration, "frequent_job_runs": frequent_job_runs}

    def compose_email_body(self, exceeded_duration, frequent_job_runs):
        email_body = ""

        email_body += "Job Runs Exceeding Duration:\n"
        for run in exceeded_duration:
            run_id = run.run_id
            start_time = datetime.utcfromtimestamp(run.start_time // 1000)
            end_time = datetime.utcfromtimestamp(run.end_time // 1000) if run.end_time else None
            runtime = self.calculate_runtime(start_time, end_time)

            email_body += f"Run ID: {run_id}, Runtime: {runtime}\n"

        email_body += "\n"

        email_body += "Frequent Job Runs:\n"
        for job_user, runs in frequent_job_runs.items():
            job_id, user_name = job_user.split('_')
            email_body += f"Job ID: {job_id}, User: {user_name}\n"

            for run in runs:
                run_id = run.run_id
                start_time = run.start_time
                end_time = run.end_time
                runtime = self.calculate_runtime(start_time, end_time)

                email_body += f"  Run ID: {run_id}, Runtime: {runtime}\n"

            email_body += "\n"

        print(email_body)

        return email_body

if __name__ == "__main__":
    load_dotenv()
    api_endpoint = os.getenv("api_endpoint")
    token = os.getenv("token")

    analyzer = DatabricksAnalyzer(api_endpoint, token)
    result = analyzer.detect_high_frequency_jobs()

    exceeded_duration = result["exceeded_duration"]
    frequent_job_runs = result["frequent_job_runs"]

    print(exceeded_duration)
    print(frequent_job_runs)
