from flask import Flask, jsonify, render_template
from data_processing import process_data
import json

app = Flask(__name__)
# app.config['MAX_CONTENT_LENGTH'] = 10 * 1024 * 1024  # 10 MB


@app.route("/")
def index():
    print("This is root")
    return render_template("index.html")


@app.route("/data", methods=["GET"])
def get_data():
    with open("data.json", "r") as f:
        results = json.load(f)
    return jsonify(results)


@app.route("/data/<date>/<time>", methods=["GET"])
def get_data_by_datetime(date, time):
    print("get_data_by_datetime")
    print(date)
    print(time)
    with open("data.json", "r") as f:
        data = json.load(f)
    filtered_data = {}
    for key, value in data.items():
        if value["date"] == date and value["time"] == time:
            filtered_data[key] = value
    print("len(filtered_data)", len(filtered_data))
    return jsonify(filtered_data)


@app.route("/top10/<date>/<time>", methods=["GET"])
def get_top10_by_datetime(date, time):
    print("get_top10_by_datetime")
    print(date)
    print(time)
    with open("top10_stations.json", "r") as f:
        data = json.load(f)
    filtered_data = []
    for row in data:
        if row["date"] == date and row["time"] == time:
            filtered_data.append(row)
    return jsonify(filtered_data)


if __name__ == "__main__":
    turnstile_file = "data/turnstile_230401.txt"
    # turnstile_file = "data/turnstile.txt"
    station_file = "data/NYSubway.csv"
    process_data(turnstile_file, station_file)
    app.run()