#!/bin/bash

aws s3 cp target/river-amazonsqs.zip s3://albogdano --storage-class REDUCED_REDUNDANCY --acl public-read