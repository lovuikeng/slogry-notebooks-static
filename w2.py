import marimo

__generated_with = "0.11.5"
app = marimo.App(width="full")


@app.cell
def _(alt, bf, mo):
    bc = mo.ui.altair_chart(
        alt.Chart(bf.value).mark_point().encode(
            y = 'count()',
            x = 'timestamp',
            color = 'conn_mode',
            shape = 'nats'
        )
    )
    bc
    return (bc,)


@app.cell
def _(bdf, mo):
    bf = mo.ui.dataframe(bdf.head(10000))
    bf
    return (bf,)


@app.cell
def _(bdf):
    bdf
    return


@app.cell
def _(build_benthos_df, mo):
    path_to_csv = mo.notebook_location() / "public" / "benthos-20250101-a.csv"
    bdf = build_benthos_df(path_to_csv)
    return bdf, path_to_csv


@app.cell
def _():
    import numpy as np
    return (np,)


@app.cell
def _():
    import pandas as pd
    return (pd,)


@app.cell
def _(np, pd):
    date_format = "%b %d, %Y @ %H:%M:%S.%f"

    def build_benthos_df(datafile: str):
        def map_conn_mode(value):
            if "app_conn" in value or "station_conn" in value:
                return "conn"
            elif "app_disconn" or "station_disconn" in value:
                return "disconn"
            return np.nan

        def map_client_type(value):
            if "device-ap" in value:
                return "ap"
            elif "device-icx" in value:
                return "icx"
            elif "se_" in value:
                return "edge"
            else:
                return "app"

        def map_disconn_reason(value):
            if "app_disconn" in value and "Client Closed" in value:
                return "Client Closed"
            if "app_disconn" in value and "Stale Connection" in value:
                return "Stale Connection"
            if "app_disconn" in value and "Write Deadline" in value:
                return "Slow Consumer"
            if "app_disconn" in value and "Read Error" in value:
                return "Read Error"
            if "station_disconn" in value and "Write Deadline" in value:
                return "Slow Consumer"
            return np.nan

        def map_conn_mode_client_type(value):
            if "app_conn" in value and "device-ap" in value:
                return "ap-conn"
            if "app_conn" in value and "device-icx" in value:
                return "icx-conn"
            if "app_disconn" in value and "device-ap" in value:
                return "ap-disconn"
            if "app_disconn" in value and "device-icx" in value:
                return "icx-disconn"
            if "station_conn" in value:
                return "station-conn"
            if "station_disconn" in value:
                return "station-disconn"
            return "other"

        def map_client_name(value):
            str_split = value.split(")(")
            if len(str_split) > 1:
                str_split = str_split[0].split("(")
                return str_split[1]
            return np.nan

        def map_nats_server(value):
            str_split = value.split("thirdparty-nats-")
            if len(str_split) > 1:
                str_split = str_split[1].split(")")
                # print(str_split[0])
                if len(str_split[0]) > 1:
                    str_split = str_split[0].split(".")
                return "nats-" + str_split[0][:1]
            #print(str_split[0])
            return "N/A" #"N/A" #np.nan

        benthos_df = pd.read_csv(datafile)
        benthos_df["timestamp"] = pd.to_datetime(benthos_df["@timestamp"], format=date_format) #.dt.floor('s')
        #benthos_df.index = benthos_df["timestamp"]
        benthos_df = benthos_df.rename(columns={'kubernetes.pod_name': 'server'})
        benthos_df['conn_mode'] = benthos_df['message'].map(map_conn_mode)
        benthos_df['client_type'] = benthos_df['message'].map(map_client_type)
        benthos_df['disconn_reason'] = benthos_df['message'].map(map_disconn_reason)
        benthos_df['client_name'] = benthos_df['message'].map(map_client_name)
        benthos_df['nats'] = benthos_df['message'].map(map_nats_server)
        replacements = {'nats-0': 'n0', 'nats-1': 'n1', 'nats-2': 'n2', 'nats-3': 'n3', 'nats-4': 'n4'}
        benthos_df['nats'] = benthos_df['nats'].map(replacements).fillna(benthos_df['nats'])
        benthos_df = benthos_df[['timestamp', 'message', 'conn_mode', 'client_type', 'disconn_reason', 'client_name', 'nats', 'server']]

        return benthos_df
    return build_benthos_df, date_format


@app.cell
def _():
    import marimo as mo
    import altair as alt
    return alt, mo


if __name__ == "__main__":
    app.run()
