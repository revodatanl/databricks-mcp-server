get_table_details_mask = {
    "name": {},
    "schema_name": {},
    "catalog_name": {},
    "comment": {},
    "columns": {"name": {}, "type_text": {}, "comment": {}},
}

get_jobs_mask = {
    "job_id": {},
    "settings": {"name": {}, "description": {}},
    "tables": [],
}

get_jobs_details_mask = {
    "job_id": {},
    "name": {},
    "creator_user_name": {},
    "run_as_user_name": {},
    "settings": {
        "continuous": {"pause_status": {}},
        "deployment": {"kind": {}},
        "format": {},
        "parameters": {"default": {}, "name": {}},
        "schedule": {
            "pause_status": {},
            "quartz_cron_expression": {},
            "timezone_id": {},
        },
        "tasks": [],
    },
}

get_jobs_runs_mask = {
    "job_id": {},
    "run_id": {},
    "creator_user_name": {},
    "state": {"life_cycle_state": {}, "result_state": {}, "state_message": {}},
    "job_arameters": {"name": {}, "default": {}},
    "start_time": {},
    "run_duration": {},
    "trigger": {},
    "run_type": {},
    "status": {"state": {}, "termination_details": {"type": {}, "message": {}}},
}
