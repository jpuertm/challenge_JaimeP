FROM python:3.7

RUN mkdir -p /Challenge_Compliance_JaimeP

WORKDIR /Challenge_Compliance_JaimeP

# Install dependencies
RUN apt-get update && apt-get install -y \
        python3.7 \
        python3-pip
		
RUN pip3 install mysql-connector

COPY sources/main.py /Challenge_Compliance_JaimeP
COPY sources/config.ini /Challenge_Compliance_JaimeP
COPY sources/dblist.json /Challenge_Compliance_JaimeP
COPY sources/user_manager.csv /Challenge_Compliance_JaimeP

ENTRYPOINT python /Challenge_Compliance_JaimeP/main.py
