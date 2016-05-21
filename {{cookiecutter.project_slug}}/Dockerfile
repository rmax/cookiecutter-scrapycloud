FROM scrapinghub/scrapinghub-stack-scrapy:1.1

WORKDIR /app
ADD . /app
RUN pip --no-cache-dir --disable-pip-version-check install -r /app/requirements.txt

CMD ["bash"]
