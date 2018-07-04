FROM python:3.6.1
ENV TAG v2.1

WORKDIR /app
RUN mkdir -p /etc/dpkg/dpkg.cfg.d \
    &&  echo "path-exclude=/usr/share/locale/*" >> /etc/dpkg/dpkg.cfg.d/excludes \
    &&  echo "path-exclude=/usr/share/man/*" >> /etc/dpkg/dpkg.cfg.d/excludes \
    &&  echo "path-exclude=/usr/share/doc/*" >> /etc/dpkg/dpkg.cfg.d/excludes \
    &&  apt-get -qq update \
    &&  apt-get -y install --no-install-recommends \
        build-essential \
        libffi-dev \
        libssl-dev \
        curl \
        git \
        vim-tiny \
        nano \
    && rm -rf /var/lib/apt/lists/* /usr/share/doc/* /usr/share/man/* /usr/share/locale/*
RUN addgroup --gid 10001 app
RUN adduser \
      --gid 10001 \
      --uid 10001 \
      --home /app \
      --shell /usr/sbin/nologin \
      --no-create-home \
      --disabled-password \
      --gecos we,dont,care,yeah \
      app


ADD . /app
RUN chown -R app:app /app


USER app
RUN python -m pip --no-cache-dir install --user -r requirements.txt
ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["start"]
