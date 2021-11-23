# Cytomine-project-migrator

> A Python utility to migrate projects between Cytomine instances.

[![GitHub release](https://img.shields.io/github/release/Cytomine-ULiege/Cytomine-project-migrator.svg)](https://github.com/Cytomine-ULiege/Cytomine-project-migrator/releases)
[![GitHub](https://img.shields.io/github/license/Cytomine-ULiege/Cytomine-project-migrator.svg)](https://github.com/Cytomine-ULiege/Cytomine-project-migrator/blob/master/LICENSE)

## Overview

This utility currently exports/imports:
* The project
* The ontology, including terms and hierarchy
* The users involved in the project
* The images involved in the project
* The human annotations in the project
* The metadata (properties, attached files and description) related to these resources.

If a project with the same name already exists in the destination instance, a (x) is added where x is a natural number.

If an ontology with the same name and the same terms already exists in the destination instance, this ontology is used. Otherwise a new one is created.

If a user with the same username, first name, last name and email already exists in the destination instance, this user is used. Otherwise a new one is created.

If an image with the same filename and dimensions already exists in the destination instance, this image is used. Otherwise a new one is uploaded and created.

## Usage

### Export a project

From the command line:
```bash
python exporter.py --host CYTOMINE_HOST --public_key PUB_KEY --private_key PRIV_KEY --id_project ID --working_path /home
```

### Import a project
From the command line:
```bash
python importer.py --host CYTOMINE_HOST --public_key PUB_KEY --private_key PRIV_KEY --project_path /home/MY_PROJECT.tar.gz
```

## References

When using our software, we kindly ask you to cite our website url and related publications in all your work (publications, studies, oral presentations,...). In particular, we recommend to cite (Marée et al., Bioinformatics 2016) paper, and to use our logo when appropriate. See our license files for additional details.

- URL: http://www.cytomine.org/
- Logo: [Available here](https://cytomine.coop/sites/cytomine.coop/files/inline-images/logo-300-org.png)
- Scientific paper: Raphaël Marée, Loïc Rollus, Benjamin Stévens, Renaud Hoyoux, Gilles Louppe, Rémy Vandaele, Jean-Michel Begon, Philipp Kainz, Pierre Geurts and Louis Wehenkel. Collaborative analysis of multi-gigapixel imaging data using Cytomine, Bioinformatics, DOI: [10.1093/bioinformatics/btw013](http://dx.doi.org/10.1093/bioinformatics/btw013), 2016. 

## License

Apache 2.0