FROM python:3.13-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py .
COPY static ./static
RUN useradd --system --no-create-home erik \
    && mkdir /data && chown erik:erik /data
USER erik
ENV ERIK_DATA=/data TZ=Europe/Stockholm
EXPOSE 8080
HEALTHCHECK --interval=5m --timeout=10s --start-period=2m --retries=3 \
  CMD python -c "import os,time,sys; p=os.environ['ERIK_DATA']+'/heartbeat'; sys.exit(0 if os.path.exists(p) and time.time()-os.path.getmtime(p)<600 else 1)"
CMD ["python", "-u", "app.py"]
