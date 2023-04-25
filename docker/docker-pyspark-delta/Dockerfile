FROM bitnami/spark:3.3.2

RUN pip install delta-spark==2.3.0 \
	&& pip install pandas
	
ENV PATH $PATH:/.local/bin
