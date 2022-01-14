import pandas as pd
import streamlit as st
from minio import Minio
import joblib
import matplotlib.pyplot as plt
from pycaret.classification import load_model, predict_model

# Downloading the Data Lake files
client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

client.fget_object("curated", "model.pkl", "model.pkl")
client.fget_object("curated", "dataset.csv", "dataset.csv")
client.fget_object("curated", "cluster.joblib", "cluster.joblib")

var_model = "model"
var_model_cluster = "cluster.joblib"
var_dataset = "dataset.csv"

# Load the trained model
model = load_model(var_model)
model_cluster = joblib.load(var_model_cluster)

# Load the dataset
dataset = pd.read_csv(var_dataset)

# Set title
st.title("Human Resource Analytics")

# Set subtitle
st.markdown("This is a Data App used to display the Machine Learning solution to the Human Resource Analytics problem.")

# Prints the used dataset
st.dataframe(dataset.drop("turnover", axis=1).head())

# grupos de empregados.
kmeans_colors = ['green' if c == 0 else 'red' if c ==
                 1 else 'blue' for c in model_cluster.labels_]

# Set a sidebar
st.sidebar.subheader("Define employee attributes for turnover prediction")

# Mapping user data to each attribute
satisfaction = st.sidebar.number_input(
    "satisfaction", value=dataset["satisfaction"].mean())
evaluation = st.sidebar.number_input(
    "evaluation", value=dataset["evaluation"].mean())
averageMonthlyHours = st.sidebar.number_input(
    "averageMonthlyHours", value=dataset["averageMonthlyHours"].mean())
yearsAtCompany = st.sidebar.number_input(
    "yearsAtCompany", value=dataset["yearsAtCompany"].mean())

# Set a button to make prediction
btn_predict = st.sidebar.button("Make prediction")

# Check if the button has been pressed
if btn_predict:
    # Set the attributes
    test_data = pd.DataFrame()
    test_data["satisfaction"] = [satisfaction]
    test_data["evaluation"] = [evaluation]
    test_data["averageMonthlyHours"] = [averageMonthlyHours]
    test_data["yearsAtCompany"] = [yearsAtCompany]

    st.subheader("Model prediction:")

    # Makes the prediction
    result = predict_model(model, data=test_data)

    result_class = result["Label"][0]
    prob = result["Score"][0]*100

    if result_class == 1:
        st.write(
            f"The model prediction for the test sample is evasion with the probability value: {round(prob, 2)}%")
    else:
        st.write(
            f"The model prediction for the test sample is permanence with the probability value: {round(prob, 2)}%")

    fig = plt.figure(figsize=(10, 6))
    plt.scatter(x="satisfaction", y="evaluation", data=dataset[dataset.turnover == 1],
                alpha=0.25, color=kmeans_colors)

    plt.xlabel("Satisfaction")
    plt.ylabel("Evaluation")

    plt.scatter(x=model_cluster.cluster_centers_[
                :, 0], y=model_cluster.cluster_centers_[:, 1], color="black", marker="X", s=100)

    plt.scatter(x=[satisfaction], y=[evaluation],
                color="yellow", marker="X", s=300)

    plt.title("Satisfaction vs Evaluation ")
    plt.show()
    st.pyplot(fig)
