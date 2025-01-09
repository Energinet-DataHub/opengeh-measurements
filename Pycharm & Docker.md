---
tags: [Setup]
title: Pycharm & Docker
created: '2024-07-09T08:13:13.189Z'
modified: '2025-01-07T11:43:53.311Z'
---

# Pycharm & Docker
Create the docker image in terminal
```
docker build -t wholesale_docker .docker\.
```
When the image is created go to:
  Python interpreter-> Add Interpreter -> On Docker ->
  Image: Pull or use existing | Image tag: wholesale_docker (should appear as an option) ->
  Next -> Next -> Conda Environment and Create.

Now whenever you have changes to the dockerfile simply remove the image wholesale_docker and build it again by running the build command.

If python interpreter does not work as inteded, meaning you are seeing red imports, simply click on the interpreter in the buttom right corner of PyCharm (/opt/conda) and click on /opt/conda


cb comments:
Jeg gav imaget et mere sigende navn.
Kørte derefter denne - men jeg kan ikke afvikle tests, fordi den nu er inde i docker containeren:
Hver gang vi kører en test, så vil vi så spinne en docker container op for at få noget execution

docker run -it measurements_docker /bin/bash
