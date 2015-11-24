FROM python:2

WORKDIR /stolos/
ADD ./setup.py /stolos/setup.py
ADD ./bin/stolos-submit /stolos/bin/stolos-submit
RUN cd /stolos/ && pip install .[testing,redis] && python setup.py develop
# assumes code base mounted as volume
CMD /stolos/bin/test_stolos
