# 🚀 Marketing Attribution Engine: The "True-Value" Pipeline

![Python](https://img.shields.io/badge/Python-3.10%2B-blue)
![dbt](https://img.shields.io/badge/dbt-BigQuery-orange)
![BigQuery](https://img.shields.io/badge/Google-BigQuery-blue)
![Streamlit](https://img.shields.io/badge/Frontend-Streamlit-red)

## 📋 Executive Summary
**The Problem:** Most companies rely on Google Analytics (GA4) "Last-Click" reporting, which blindly credits the final touchpoint (often "Direct" or "Branded Search") for a sale. This biases data against top-of-funnel channels like Facebook and TikTok, leading to wasted budget and inefficient scaling.

**The Solution:** I built an end-to-end **ELT (Extract, Load, Transform)** pipeline that ingests raw touchpoint data, uses **dbt** to reconstruct user journeys, and applies a custom **Markov Chain Probabilistic Model** to calculate the "Removal Effect" of each channel.

**The Result:** The model revealed that Social Ads were **undervalued by 100%** in traditional reporting, identifying significant hidden revenue opportunities.

---

## 🏗️ Architecture & Tech Stack

This project simulates a production-grade **Modern Data Stack (MDS)** environment.

```mermaid
graph LR
    A[Raw Data Generation] -->|Python Script| B(BigQuery Raw Tables)
    B -->|dbt| C{dbt Transformation}
    C -->|SQL Window Functions| D[Sessionized User Journeys]
    D -->|Python| E[Custom Markov Model]
    E -->|Streamlit| F[Interactive Dashboard]
