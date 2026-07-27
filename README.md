Tugas ini dibuat dengan tujuan sebagai berikut:

- Mampu menggunakan Apache Airflow.

- Mampu melakukan validasi data dengan menggunakan Great Expectations.

- Mampu mempersiapkan data untuk digunakan sebelum masuk ke database NoSQL.

- Mampu mengolah data menggunakan library PySpark.
---

## Assignment Problems

Sebagai Data Engineer, kamu diminta untuk membuat data pipeline automation.

---
## Dataset

### Ketentuan Dataset
1. Pilihlah dataset yang paling nyaman digunakan dalam mengerjakan tugas. Adapun ketentuan dataset yang harus digunakan adalah :
   * Setidaknya terdapat minimal 10 column.
   * Setiap column terdiri dari :
     + Capital letter dan lower letter atau semua huruf merupakan capital letter.
     + Contoh : `Age`, `fullName`, `CITY`, `Education Level`.
   * Tidak diperbolehkan memilih dataset dimana nama column terdiri dari lower letter saja.
   * Terdapat campuran column berbentuk kategorikal dan numerikal. Sebisa mungkin jumlah kolom kategorikal dan numerikal seimbang, tidak ada yang terlalu mendominasi.

2. Anda dapat membayangkan bahwa Anda saat ini bekerja sebagai seorang Data Engineer disebuah perusahaan. Carilah dataset yang sekiranya merepresentasikan domain sebuah perusahaan seperti data mengenai : 
   - Product inventory.
   - Revenue & profit.
   - Kinerja pegawai.
   - Customer profile.
   - Keluhan produk/jasa.
   - dll.

### Data Sources
Student dapat memilih dataset dari salah satu repository dibawah ini. Popular open data repositories :

- [UC Irvine Machine Learning Repository](https://archive.ics.uci.edu/ml/index.php)
- [Kaggle datasets](https://www.kaggle.com/datasets)
- [Amazon’s AWS datasets](https://registry.opendata.aws/)

Meta portals :

- [Data Portals](http://dataportals.org/)
- [OpenDataMonitor](https://opendatamonitor.eu/frontend/web/index.php?r=dashboard%2Findex)
- [Quandl](https://www.quandl.com/)
- Sumber lain yang kredibel.

---

## Assignment Instructions
Buatlah sistem automasi data pipeline (ETL) dengan mengkombinasikan penggunaan PySpark, Airflow, dan MongoDB.

### A. Pra Automasi
1. Tentukan dataset yang hendak dipakai. Beri nama dataset ini dengan `P2M3_<nama-student>_data_raw.csv`. Contoh : `P2M3_raka_ardhi_data_raw.csv`.
2. Sebelum membuat sistem automasi, kamu perlu melakukan eksplorasi data dan validasi data di **notebook (.ipynb)**.
3. Pada tahapan ini, kamu boleh menggunakan Pandas (**NOTE:** perlu diperhatikan bahwa jika ukuran data cukup besar, penggunaan Pandas tidak lagi efisien dan perlu menggunakan PySpark, namun untuk kasus ini kamu bisa ambil sampel data saja).
4. Lakukan eksplorasi data sederhana untuk mengetahui kondisi data dan kesimpulan yang ingin didapatkan adalah langkah-langkah data cleaning dan processing yang nantinya akan dikembangkan di data pipeline.
5. Selanjutnya lakukan validasi data menggunakan Great Expectations. Adapun kriteria mengenai Expectation yang dipilih adalah :
   - Lakukan minimal 7 Expectations yang didalamnya harus ada Expectation untuk:
     + to be unique
     + to be between min_value and max_value
     + to be in set
     + to be in type list
     + 3 jenis Expectation yang berbeda yang tidak diajarkan pada lecture Data Ethics & Data Validation
   - Kamu diwajibkan untuk menerapkan 4 Expectation yang telah ditentukan dan 3 Expectation lain yang tidak diajarkan pada lecture dimana semua Expectation ini berbeda satu sama lain.
   - Ketujuh Expectation yang digunakan haruslah semuanya bernilai `success: true`.
   - Untuk Expectation `to be unique`, Kamu diizinkan untuk membuat sebuah column baru jika dirasa column yang ada didataset tidak ada yang unik. Column baru ini haruslah berasal dari column yang sudah ada. Kamu dapat membuat gabungan antara beberapa column yang sudah ada untuk column baru ini. Buatlah skenario fiksi mengenai kegunaan dari column ini sehingga jelas peruntukannya.
   - Setiap Expectation hanya boleh berada pada 1 cell yang berbeda-beda sehingga dapat dilihat mengenai hasilnya.
   - Simpan Python Notebook dengan nama `P2M3_<nama-student>.ipynb`. Contoh : `P2M3_raka_ardhi.ipynb`.
  
### B. Automasi Data Pipeline
Buat proses extract, transform, dan load dalam file python script yang terpisah.
#### 1. Extract
- Buat file `extract.py` untuk membaca data saja.
- Gunakan  PySpark untuk membaca data.
- Buat script load data dalam bentuk function untuk memudahkan dan outpunya berupa Spark Dataframe yang nantinya akan dibaca oleh file transform.py
- Contoh:
  ```python
  from pyspark.sql import SparkSession
  def load_data(file_path):
      spark = SparkSession.builder.getOrCreate()
      data = spark.read.csv(file_path, header=True, inferSchema=True)
      return data```

#### 2. Transform
- Buat file `transform.py` untuk memproses data yang telah diekstraksi.
- Tahapan data cleaning dan processing hanya dilakukan dengan PySpark dan tahapannya sesuai dengan kesimpulan yang didapatkan dari eksplorasi data yang sudah dilakukan di notebook.
- Pastikan hasil transformasi data siap digunakan untuk proses selanjutnya.
- Contoh:
  ```python
  def transform(data):
    data_dropped = data.drop("col1")
    return data_dropped
  ```

#### 3. Load
- Buat file `load.py` untuk menyimpan data ke database Mongodb
- Buat koneksi ke database Mongodb dengan pymongo
- Simpan Pyspark dataframe ke database Mongodb
- Cek di Mongodb atlas apakah database sudah tersimpan dan screenshot. Push screenshotnya ke repository dengan nama file `P2M3_<nama-student>_screenshot_mongo.jpg`. Contoh: `P2M3_raka_ardhi_screenshot_mongo.jpg`


### Workflow Orchestration
Lakukan automasi dengan membuat DAG dengan kriteria :
   - DAG berisi 3 node/task dibawah ini :
     + `Extract` : berisi script untuk menjalankan file extract.py dan fungsi di dalamnya yang bertujuan untuk load data.
     + `Transform` : berisi script untuk menjalankan file transform.py dan fungsi di dalamnya yang bertujuan untuk transformasi data.
     + `Load` : berisi script untuk menjalankan file load.py dan fungsi di dalamnya yang bertujuan untuk menyimpan data ke database MongoDB.
   - Penjadwalan dimulai pada tanggal 01 November 2024.
   - Penjadwalan dilakukan setiap hari Sabtu jam 09:10 AM hingga 09:30 AM dan dilakukan per 10 menit. Tabel dibawah ini adalah contoh hasil penjadwalannya :
     | No | Execution Time |
     | --- | --- |
     | 1 | Saturday, 2 November 2024 09:10 AM |
     | 2 | Saturday, 2 November 2024 09:20 AM |
     | 3 | Saturday, 2 November 2024 09:30 AM |
     | 4 | Saturday, 9 November 2024 09:10 AM |
     | 5 | Saturday, 9 November 2024 09:20 AM |
     | 6 | Saturday, 9 November 2024 09:30 AM |
     | 7 | Saturday, 16 November 2024 09:10 AM |
     | 8 | Saturday, 16 November 2024 09:20 AM |
     | 9 | ... |
   - Simpan DAG dengan nama `P2M3_raka_ardhi_DAG.py`.
   - Jalankan penjadwalan yang Anda buat. Lalu, screenshot mengenai graph DAG yang telah Anda jalankan dengan nama `P2M3_raka_ardhi_DAG_graph.jpg`. *([Ilustrasi graph DAG](https://i.sstatic.net/KgUwc.png))*
