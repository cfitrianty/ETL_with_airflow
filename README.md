# ETL_with_airflow

Ini merupakan repository untuk membangun pipeline ETL.
Adapun 3 file yang berhubungan yaitu :
1. Assignment_db_etl.py = berisi schedule dan 1 dag dengan 2 task untuk meng-extract data dari source dan meng-load data ke db target
2. Assignment_notification_email.py = berisi schedule dan script pengiriman output ke email, dengan 1 dag dan 1 task untuk meng-agregat perhitungan BMI
3. Assignment_trigger_dag.py = berisi 1 dag dengan 3 task, task pertama akan menjalankan dag file pertama "Assignment_db_etl" sesuai schedule nya, task kedua berisi sensor yang menunggu dag pertama telah selesai running, task ketiga akan mentrigger dag file kedua "assignment_notification_email" sesuai schedule nya.
