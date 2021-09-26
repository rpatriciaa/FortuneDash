import pandas as pd

def set_to_dataframe(ti):
    data = ti.xcom_pull(task_ids=['select_data'])
    df = pd.DataFrame(data=data)

    return df

