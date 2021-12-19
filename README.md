# ProtecTor

This work aims at implementing an anomaly detection and alerting system for the Tor network. A prototype-oriented, independent component-based design has been used for its development and deployment.

The system is composed of several modules that allow obtaining telemetry data published by the Tor project, processing, storing, and analyzing them through a modular anomaly analysis component and issuing alerts through a bidirectional channel in the form of a bot based on the Telegram platform that, additionally, allows interacting with the system through multiple commands.

It also adds data visualization through dashboards served by Grafana and a plotting module developed to visualize graphs through the bot.

The result is a functional, modular and extensible platform that allows any interested person or group of interest to perform a simple deployment thanks to the Docker container-based implementation of the entire system.
This work is the result of the assimilation of multiple competences and has been released under GPL license. It has been carried out satisfactorily in all its stages, presents a high degree of maturity and allows its exploitation in productive environments.

However, it is recommended that the analysis component be extended in the form of modules that implement additional anomaly detection algorithms to allow a comparative analysis of the different alerts that may occur.

## Install
---

`git clone --depth=1 https://github.com/deibit/protector`

edit `.env_sample and` save as `.env`

`docker-compose up`
