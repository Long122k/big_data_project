# Big data project
### Giới thiệu bài toán
- Bài toán: Crawl dữ liệu thời gian thực từ 1 api giá vàng và phân tích, đánh giá, dự đoán về giá mua, bán.​
- Bài toán được thực hiện trên cụm 3 máy ảo
- Luồng dữ liệu: Crawl data từ api giá vàng về máy sử dụng kafka. Sau đó lưu dữ liệu vào trong HDFS. Spark lấy dữ liệu từ HDFS, dùng SparkML để dự đoán giá mua và bán đồng thời sử dụng SparkSQL để kiểm tra một số thuộc tính của dữ liệu. Dữ liệu được visualize trên jupyter notebook.​
![pipeline](./image/pipeline.jpg)

### Dữ liệu
Crawl trực tiếp từ api giá vàng:

https://forex-data-feed.swissquote.com/public-quotes/bboquotes/instrument/XAU/USD


### Kafka​
- Cụm kafka gồm 3 brokers.
- 1 topic tên là Goldrates để crawl data dạng json về máy.

Sau đó chuyển data từ dạng json sang csv.
### Hadoop HDFS
- Gồm 1 máy master và 2 slave.

    ![data_node](./image/data_node.jpg)
- Lưu trữ các file data đã crawl được như hình dưới:

    ![hdfs](./image/hdfs.jpg)

### Spark
- Cụm spark gồm 1 máy master và 2 worker.

    ![spark](./image/spark.jpg)

- Khởi động jupyter notebook, tạo 1 job tên là Bigdata66 lấy dữ liệu từ hdfs.

    ![spark_job](./image/spark_job.jpg)

### Spark ML
- Vecto hóa dữ liệu:​

    ![vector](./image/vector.jpg)

- Chia dữ liệu thành 2 tập:

    ![split_data](./image/split_data.jpg)

- Sử dụng Linear Regression để dự đoán bid và ask​:

    ![train](./image/train.jpg)

- Kết qủa dự đoán:

    ![predict](./image/predict.png)

### Spark SQL
- Tạo TempView goldPrice:
    ![tempView](./image/tempView.jpg)
- Kiểm tra 1 vài thuộc tính của dữ liệu:
    ![sql](./image/sql.jpg)

