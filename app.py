import os
import boto3
import requests
from PIL import Image
from io import BytesIO
from botocore.exceptions import NoCredentialsError
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
import re
import time
from datetime import datetime
from collections import OrderedDict
from flask import Flask, request, jsonify
from urllib.parse import quote as url_quote

from flask_cors import CORS, cross_origin

app = Flask(__name__)
CORS(app)
os.environ["AWS_ACCESS_KEY_ID"] = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")

os.environ["AWS_SECRET_ACCESS_KEY"] = os.environ.get("AWS_SECRET_ACCESS_KEY")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")


def get_unique_elements(input_list):
    return list(OrderedDict.fromkeys(input_list))


def convert_images_to_pdf(folder_name, id, title):
    id = int(int(id) / 100)
    images = [Image.open(os.path.join(folder_name, file)) for file in os.listdir(folder_name) if file.endswith(('png', 'jpg', 'jpeg', 'webp'))]
    if images:
        pdf_buffer = BytesIO()
        images[0].save(pdf_buffer, format='PDF', save_all=True, append_images=images[1:])
        pdf_buffer.seek(0)
        print("PDF created in memory")
        return pdf_buffer, f"{title}_{id}.pdf"
    return None, None


def folder_exists_on_s3(bucket, folder_name, aws_access_key_id, aws_secret_access_key, region_name):
    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name
    )
    result = s3.list_objects_v2(Bucket=bucket, Prefix=folder_name)
    return 'Contents' in result


def download_images_to_s3(bucket, folder_name, image_urls, aws_access_key_id, aws_secret_access_key, region_name, id, title):
    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name
    )

    print(f'Downloading images to S3 for {folder_name}')
    local_folder_path = os.path.join(os.getcwd(), folder_name.replace("\\", "/"))
    os.makedirs(local_folder_path, exist_ok=True)

    for index, url in enumerate(image_urls):
        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()

            image_name = f"image_{index + 1}.jpg"
            local_image_path = os.path.join(local_folder_path, image_name)

            with open(local_image_path, 'wb') as out_file:
                for chunk in response.iter_content(chunk_size=8192):
                    out_file.write(chunk)

            s3_key = f"{folder_name}/{image_name}"
            with open(local_image_path, 'rb') as data:
                s3.upload_fileobj(data, bucket, s3_key)

            print(f"Downloaded {url} to s3://{bucket}/{s3_key}")
        except requests.exceptions.RequestException as e:
            print(f"Failed to download {url}: {e}")
        except NoCredentialsError:
            print("Credentials not available")

    pdf_buffer, pdf_name = convert_images_to_pdf(local_folder_path, id, title)
    s3_key_ = f"{folder_name}/{pdf_name}"
    if pdf_buffer:
        s3.upload_fileobj(pdf_buffer, bucket, s3_key_)
        print(f"PDF uploaded to s3://{bucket}/{s3_key_}")


def scrape_images(country, region):
    AWS_DEFAULT_REGION = 'ap-south-1'
    AWS_BUCKET = 'needsandwants'
    folder_name = 'advertisement-flyer'

    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")

    service = Service(r'chromedriver.exe')

    driver = webdriver.Chrome(executable_path=r'chromedriver.exe', options=chrome_options)

    url = f"https://d4donline.com/en/{country}/{region}/offers"

    driver.get(url)
    time.sleep(5)

    flairs = driver.find_elements(By.CLASS_NAME, 'grid-container')
    all_flair_links = flairs[0].find_elements(By.TAG_NAME, 'a')

    for flair_link in all_flair_links:
        title = flair_link.get_attribute('title')
        trimmed_title = "_".join(title.split()[:8])
        trimmed_title = trimmed_title[:10]

        href = flair_link.get_attribute('href')
        pattern = re.compile(r'/(\d+)/')
        unique_id = pattern.search(href).group(1)

        base_folder = os.path.join(folder_name, trimmed_title, f"{datetime.now().strftime('%Y-%m-%d')}_{unique_id}").replace('\\', '/')
        if folder_exists_on_s3(AWS_BUCKET, base_folder, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION):
            print(f"Skipping scraping for {unique_id} as folder already exists on S3.")
            continue

        html = requests.get(href).text
        soup = BeautifulSoup(html, 'html.parser')

        images = []
        for each_picture in soup.find_all('picture', attrs={'class': 'offer-page'}):
            img_tag = each_picture.find('img')
            if img_tag:
                try:
                    img_src = img_tag['src']
                except:
                    img_src = img_tag['data-page-src']
                images.append(img_src)

        unique_images = get_unique_elements(images)

        download_images_to_s3(AWS_BUCKET, base_folder, unique_images, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION, unique_id, trimmed_title)

    driver.quit()


@app.route('/scrape_images', methods=['POST'])
def scrape_images_api():
    data = request.json
    country = data.get('country')
    region = data.get('region')
    if not country or not region:
        return jsonify({"error": "Country and region are required parameters"}), 400

    try:
        scrape_images(country, region)
        return jsonify({"message": "Scraping completed successfully"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# if __name__ == '__main__':
#     app.run(debug=True)
