FROM scrapinghub/scrapinghub-stack-scrapy:1.2-py3

WORKDIR /app
ADD . /app
RUN set -x \
    && pip --no-cache-dir \
      --disable-pip-version-check \
      install -r /app/requirements.txt

CMD ["scrapy", "list"]
