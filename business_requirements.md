# Business Requirements & Questions

Before designing a data system, we must understand the questions the business and the end-users need answered.

## Real-World Analytics Questions

### 1. User-Facing Questions (Health & Fitness Insights)
*   **Performance Age**: Based on resting heart rate (RHR), heart rate variability (HRV), VO2 Max estimates, and daily activity levels, what is the user's biological "performance age" compared to their chronological age? 
*   **Metabolic Tracking (BMR & TDEE)**: What is the user's Basal Metabolic Rate (BMR), and how many total calories (active + resting) are they expending daily?
*   **Cardiovascular Health (HRV & RHR)**: How is the user's HRV and RHR trending over 7-day, 30-day, and 90-day windows? Is there a sudden drop indicating illness or overtraining?
*   **Sleep Architecture**: What is the ratio of Deep vs. REM vs. Light sleep over the last month? How do high-strain workout days impact the following night's sleep architecture?
*   **Fatigue & Recovery Levels**: Does the user have a high accumulated fatigue level? How many consecutive days have they sustained high cardiovascular strain without adequate recovery time?

### 2. Business-Facing Questions (Product & Strategy)
*   **Feature Adoption**: What are the most common workout types logged? What time of day are users most active?
*   **Hardware / Firmware Health**: Are certain firmware versions dropping data packets or failing to record sleep accurately? 
*   **User Retention & Churn**: Do users who consistently maintain high recovery scores retain their subscriptions longer than those who are chronically fatigued?

---

## Data Requirements

To answer the above questions, the OLTP database and Data Lake must capture:

1.  **User Profiles**: Date of birth (to calculate age), gender, height, current weight.
2.  **Daily Aggregates**: Sleep stages (seconds in deep, REM, light), resting metrics (RHR, respiratory rate, average HRV), and daily computed scores (recovery, strain).
3.  **Workout Sessions**: Granular records of workout start/end times, activity types, heart rate zones, and calories burned.
4.  **Hardware Syncs**: Audit trails of when the device synced with the phone.
5.  **Unstructured Device Payloads**: Raw JSON files transmitted from the Bluetooth device. In the real world, devices often send compressed JSON/Protobuf files to an API, which dumps them into an Object Store (GCS) before parsing them into the SQL database.

---

## OLTP Database Schema (PostgreSQL via Cloud SQL)

We will use a normalized relational schema for the transactional database:

*   **`users`**: core demographic info.
*   **`devices`**: hardware tracking.
*   **`daily_metrics`**: day-level rollups computed locally on the device/phone.
*   **`workouts`**: discreet exercise sessions.

*(Note: High-frequency time-series data like second-by-second heart rate is typically stored in a NoSQL database like Bigtable or directly in GCS, not in a standard relational OLTP schema, so we will generate that as unstructured/semi-structured files for the data lake).*
