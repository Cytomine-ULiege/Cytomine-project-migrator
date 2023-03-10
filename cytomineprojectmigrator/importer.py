"""Cytomine Project Importer

    Copyright (c) 2009-2023. Authors: see NOTICE file.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

         http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import copy
import json
import logging
import os
import random
import requests
import shutil
import string
import sys
import tarfile
import time

from argparse import ArgumentParser
from enum import Enum

import requests
from joblib import Parallel, delayed

from cytomine import Cytomine
from cytomine.models import (
    AbstractImage,
    AbstractImageCollection,
    Annotation,
    AnnotationCollection,
    AttachedFile,
    Description,
    ImageInstance,
    ImageInstanceCollection,
    Model,
    Ontology,
    OntologyCollection,
    Project,
    ProjectCollection,
    Property,
    RelationTerm,
    StorageCollection,
    Term,
    TermCollection,
    User,
    UserCollection,
)

__author__ = "Rubens Ulysse <urubens@uliege.be>"


class Models(str, Enum):
    """Cytomine Model enumeration"""

    DESCRIPTION = "description"
    FILE = "attached-files"
    IMAGE_INSTANCE = "imageinstance-collection"
    ONTOLOGY = "ontology"
    PROJECT = "project"
    PROPERTY = "properties"
    TERM = "term-collection"
    USER_ANNOTATION = "user-annotation-collection"
    USER = "user-collection"


def find_first(l):
    """Find the first item in the list and return it"""
    return l[0] if len(l) > 0 else None


def random_string(length=10):
    """Generate a random string"""
    return "".join(random.choice(string.ascii_letters) for _ in range(length))


def connect_as(user=None, open_admin_session=False):
    """Connect to Cytomine with a user"""
    public_key = None
    private_key = None

    if hasattr(user, "publicKey") and user.publicKey:
        public_key = user.publicKey

    if hasattr(user, "privateKey") and user.privateKey:
        private_key = user.privateKey

    if not public_key or not private_key:
        keys = user.keys()
        public_key, private_key = keys["publicKey"], keys["privateKey"]

    Cytomine.get_instance().set_credentials(public_key, private_key)
    if open_admin_session:
        Cytomine.get_instance().open_admin_session()
    return Cytomine.get_instance().current_user


class Importer:
    """Import a Cytomine Project archive to a Cytomine server"""

    def __init__(self, host_upload, working_path, with_original_date=False):
        self.host_upload = host_upload
        self.with_original_date = with_original_date
        self.id_mapping = {}

        self.working_path = working_path

        self.with_userannotations = False
        self.with_images = False

        self.super_admin = None

        self.remote_users = []
        self.remote_project = None
        self.filenames = self._get_filenames()

    def _get_filenames(self):
        """Get the filenames of the JSON"""

        filenames = [
            filename
            for filename in os.listdir(self.working_path)
            if os.path.isfile(os.path.join(self.working_path, filename))
        ]

        mapping = {model: [] for model in Models}
        for filename in filenames:
            key = next(filter(filename.startswith, Models), None)
            path = os.path.join(self.working_path, filename)

            if key in [Models.DESCRIPTION, Models.FILE, Models.PROPERTY]:
                mapping[key].append(path)
            else:
                mapping[key] = path

        return mapping

    def import_ontology(self):
        """
        Import the ontology with terms and relation terms that are stored in pickled files in working_path.
        If the ontology exists (same name and same terms), the existing one is used.
        Otherwise, an ontology with an available name is created with new terms and corresponding relationships.
        """

        ontologies = OntologyCollection().fetch()
        with open(self.filenames[Models.ONTOLOGY], "r", encoding="utf-8") as file:
            remote_ontology = Ontology().populate(json.load(file))
        remote_ontology.name = remote_ontology.name.strip()

        terms = TermCollection().fetch()
        remote_terms = TermCollection()
        with open(self.filenames[Models.TERM], "r", encoding="utf-8") as file:
            for term in json.load(file):
                remote_terms.append(Term().populate(term))

        def ontology_exists():
            compatible_ontology = find_first(
                [o for o in ontologies if o.name == remote_ontology.name.strip()]
            )
            if compatible_ontology:
                set1 = set(
                    (t.name, t.color)
                    for t in terms
                    if t.ontology == compatible_ontology.id
                )
                difference = [
                    term for term in remote_terms if (term.name, term.color) not in set1
                ]
                if len(difference) == 0:
                    return True, compatible_ontology
                return False, None

            return True, None

        i = 1
        remote_name = remote_ontology.name
        found, existing_ontology = ontology_exists()
        while not found:
            remote_ontology.name = f"{remote_name} ({i})"
            found, existing_ontology = ontology_exists()
            i += 1

        # SWITCH to ontology creator user
        connect_as(User().fetch(self.id_mapping[remote_ontology.user]))
        if not existing_ontology:
            ontology = copy.copy(remote_ontology)
            ontology.id = None
            ontology.user = self.id_mapping[remote_ontology.user]
            if not self.with_original_date:
                ontology.created = None
                ontology.updated = None
            ontology.save()
            self.id_mapping[remote_ontology.id] = ontology.id
            logging.info("Ontology imported: %s", ontology)

            for remote_term in remote_terms:
                logging.info("Importing term: %s", remote_term)
                term = copy.copy(remote_term)
                term.id = None
                term.ontology = self.id_mapping[term.ontology]
                term.parent = None
                if not self.with_original_date:
                    term.created = None
                    term.updated = None
                term.save()
                self.id_mapping[remote_term.id] = term.id
                logging.info("Term imported: %s", term)

            remote_relation_terms = [(term.parent, term.id) for term in remote_terms]
            for relation in remote_relation_terms:
                parent, child = relation
                if parent:
                    rt = RelationTerm(
                        self.id_mapping[parent], self.id_mapping[child]
                    ).save()
                    logging.info("Relation term imported: %s", rt)
        else:
            self.id_mapping[remote_ontology.id] = existing_ontology.id

            ontology_terms = [t for t in terms if t.ontology == existing_ontology.id]
            for remote_term in remote_terms:
                self.id_mapping[remote_term.id] = find_first(
                    [t for t in ontology_terms if t.name == remote_term.name]
                ).id

            logging.info("Ontology already encoded: %s", existing_ontology)

        # SWITCH USER
        connect_as(self.super_admin, True)

    def import_project(self):
        """
        Import the project (i.e. the Cytomine Project domain) stored in pickled file in working_path.
        If a project with the same name already exists, append a (x) suffix where x is an increasing number.
        """
        projects = ProjectCollection().fetch()
        with open(self.filenames[Models.PROJECT], "r", encoding="utf-8") as file:
            self.remote_project = Project().populate(json.load(file))
        self.remote_project.name = self.remote_project.name.strip()

        def available_name():
            i = 1
            existing_names = [o.name for o in projects]
            new_name = project.name
            while new_name in existing_names:
                new_name = f"{project.name} ({i})"
                i += 1
            return new_name

        project = copy.copy(self.remote_project)
        project.id = None
        project.name = available_name()
        project.discipline = None
        project.ontology = self.id_mapping[project.ontology]
        project_contributors = [
            u for u in self.remote_users if "project_contributor" in u.roles
        ]
        project.users = [self.id_mapping[u.id] for u in project_contributors]
        project_managers = [
            u for u in self.remote_users if "project_manager" in u.roles
        ]
        project.admins = [self.id_mapping[u.id] for u in project_managers]
        if not self.with_original_date:
            project.created = None
            project.updated = None
        project.save()
        self.id_mapping[self.remote_project.id] = project.id
        logging.info("Project imported: %s", project)

    def import_images(self):
        """Import the images to the project"""

        storages = StorageCollection().fetch()
        abstract_images = AbstractImageCollection().fetch()
        remote_images = ImageInstanceCollection()
        with open(self.filenames[Models.IMAGE_INSTANCE], "r", encoding="utf-8") as file:
            for image in json.load(file):
                remote_images.append(ImageInstance().populate(image))

        remote_images_dict = {}

        for remote_image in remote_images:
            image = copy.copy(remote_image)

            # Fix old image name due to urllib3 limitation
            remote_image.originalFilename = bytes(
                remote_image.originalFilename, "utf-8"
            ).decode("ascii", "ignore")
            if remote_image.originalFilename not in remote_images_dict:
                remote_images_dict[remote_image.originalFilename] = [remote_image]
            else:
                remote_images_dict[remote_image.originalFilename].append(remote_image)
            logging.info("Importing image: %s", remote_image)

            # SWITCH user to image creator user
            connect_as(User().fetch(self.id_mapping[remote_image.user]))
            # Get its storage
            storage = find_first(
                [
                    s
                    for s in storages
                    if s.user == Cytomine.get_instance().current_user.id
                ]
            )
            if not storage:
                storage = storages[0]

            # Check if image is already in its storage
            abstract_image = find_first(
                [
                    ai
                    for ai in abstract_images
                    if ai.originalFilename == remote_image.originalFilename
                    and ai.width == remote_image.width
                    and ai.height == remote_image.height
                ]
            )
            if abstract_image:
                logging.info(
                    "== Found corresponding abstract image. Linking to project."
                )
                ImageInstance(
                    abstract_image.id, self.id_mapping[self.remote_project.id]
                ).save()
            else:
                logging.info("== New image starting to upload & deploy")
                filename = os.path.join(
                    self.working_path,
                    "images",
                    image.originalFilename.replace("/", "-"),
                )
                Cytomine.get_instance().upload_image(
                    self.host_upload,
                    filename,
                    storage.id,
                    self.id_mapping[self.remote_project.id],
                )
                time.sleep(0.8)

            # SWITCH USER
            connect_as(self.super_admin, True)

        # Waiting for all images...
        n_new_images = -1
        new_images = None
        count = 0
        while n_new_images != len(remote_images) and count < len(remote_images) * 5:
            new_images = ImageInstanceCollection().fetch_with_filter(
                "project", self.id_mapping[self.remote_project.id]
            )
            n_new_images = len(new_images)
            if count > 0:
                time.sleep(5)
            count = count + 1
        print("All images have been deployed. Fixing image-instances...")

        # Fix image instances meta-data:
        for new_image in new_images:
            remote_image = remote_images_dict[new_image.originalFilename].pop()
            if self.with_original_date:
                new_image.created = remote_image.created
                new_image.updated = remote_image.updated
            new_image.reviewStart = (
                remote_image.reviewStart
                if hasattr(remote_image, "reviewStart")
                else None
            )
            new_image.reviewStop = (
                remote_image.reviewStop if hasattr(remote_image, "reviewStop") else None
            )
            new_image.reviewUser = (
                self.id_mapping[remote_image.reviewUser]
                if hasattr(remote_image, "reviewUser") and remote_image.reviewUser
                else None
            )
            new_image.instanceFilename = remote_image.instanceFilename
            new_image.update()
            self.id_mapping[remote_image.id] = new_image.id
            self.id_mapping[remote_image.baseImage] = new_image.baseImage

            new_abstract = AbstractImage().fetch(new_image.baseImage)
            if self.with_original_date:
                new_abstract.created = remote_image.created
                new_abstract.updated = remote_image.updated
            if new_abstract.magnification is None:
                new_abstract.magnification = remote_image.magnification
            new_abstract.update()

        print("All image-instances have been fixed.")

    def import_annotations(self):
        """Import the user annotations to the project"""

        remote_annots = AnnotationCollection()
        with open(
            self.filenames[Models.USER_ANNOTATION], "r", encoding="utf-8"
        ) as file:
            for annotation in json.load(file):
                remote_annots.append(Annotation().populate(annotation))

        def _add_annotation(remote_annotation, id_mapping, with_original_date):
            if (
                remote_annotation.project not in id_mapping.keys()
                or remote_annotation.image not in id_mapping.keys()
            ):
                return

            annotation = copy.copy(remote_annotation)
            annotation.id = None
            annotation.slice = None
            annotation.project = id_mapping[remote_annotation.project]
            annotation.image = id_mapping[remote_annotation.image]
            annotation.user = id_mapping[remote_annotation.user]
            annotation.term = [id_mapping[t] for t in remote_annotation.term]
            if not with_original_date:
                annotation.created = None
                annotation.updated = None
            annotation.save()

        for user in [
            u for u in self.remote_users if "userannotation_creator" in u.roles
        ]:
            remote_annots_for_user = [a for a in remote_annots if a.user == user.id]
            # SWITCH to annotation creator user
            connect_as(User().fetch(self.id_mapping[user.id]))
            Parallel(n_jobs=-1, backend="threading")(
                delayed(_add_annotation)(
                    remote_annotation, self.id_mapping, self.with_original_date
                )
                for remote_annotation in remote_annots_for_user
            )

            # SWITCH back to admin
            connect_as(self.super_admin, True)

    def import_metadata(self):
        """Import the metadata related to the project, annotation, etc"""

        obj = Model()
        obj.id = -1
        obj.class_ = ""

        for filename in self.filenames[Models.PROPERTY]:
            with open(filename, "r", encoding="utf-8") as file:
                for remote_property in json.load(file):
                    prop = Property(obj).populate(remote_property)
                    prop.id = None
                    prop.domainIdent = self.id_mapping[prop.domainIdent]
                    prop.save()

        for filename in self.filenames[Models.FILE]:
            with open(filename, "r", encoding="utf-8") as file:
                for remote_af in json.load(file):
                    af = AttachedFile(obj).populate(remote_af)
                    af.id = None
                    af.domainIdent = self.id_mapping[af.domainIdent]
                    af.filename = os.path.join(
                        self.working_path, "attached_files", remote_af.get("filename")
                    )
                    af.save()

        for filename in self.filenames[Models.DESCRIPTION]:
            with open(filename, "r", encoding="utf-8") as file:
                desc = Description(obj).populate(json.load(file))
                desc.id = None
                desc.domainIdent = self.id_mapping[desc.domainIdent]
                desc._object.class_ = desc.domainClassName
                desc._object.id = desc.domainIdent
                desc.save()

    def run(self):
        """Import a Cytomine project"""

        self.super_admin = Cytomine.get_instance().current_user
        connect_as(self.super_admin, True)

        users = UserCollection().fetch()
        self.remote_users = UserCollection()
        with open(self.filenames[Models.USER], "r", encoding="utf-8") as file:
            for user in json.load(file):
                self.remote_users.append(User().populate(user))

        for remote_user in self.remote_users:
            user = find_first([u for u in users if u.username == remote_user.username])
            if not user:
                user = copy.copy(remote_user)
                user.id = None
                if not user.password:
                    user.password = random_string(8)
                if not self.with_original_date:
                    user.created = None
                    user.updated = None
                user.save()
            self.id_mapping[remote_user.id] = user.id

        logging.info("1/ Import ontology and terms")
        self.import_ontology()

        # SWITCH USER
        connect_as(self.super_admin, True)

        logging.info("2/ Import project")
        self.import_project()

        logging.info("3/ Import images")
        self.import_images()

        logging.info("4/ Import user annotations")
        self.import_annotations()

        logging.info("5/ Import metadata (properties, attached files, description)")
        self.import_metadata()


if __name__ == "__main__":
    parser = ArgumentParser(description="Cytomine Project Importer")
    parser.add_argument(
        "--host", help="The Cytomine host on which project is imported."
    )
    parser.add_argument(
        "--host_upload", help="The Cytomine host on which images are uploaded."
    )
    parser.add_argument(
        "--public_key",
        help="The Cytomine public key used to import the project. "
        "The underlying user has to be a Cytomine administrator.",
    )
    parser.add_argument(
        "--private_key",
        help="The Cytomine private key used to import the project. "
        "The underlying user has to be a Cytomine administrator.",
    )
    parser.add_argument(
        "--project_path",
        help="The base path where the project archive is stored.",
    )
    params, _ = parser.parse_known_args(sys.argv[1:])

    with Cytomine(params.host, params.public_key, params.private_key) as _:
        options = {k: v for (k, v) in vars(params).items() if k.startswith("without")}

        if params.project_path.startswith("http://") or params.project_path.startswith(
            "https://"
        ):
            logging.info("Downloading from %s", params.project_path)
            response = requests.get(
                params.project_path, allow_redirects=True, stream=True
            )
            params.project_path = params.project_path[
                params.project_path.rfind("/") + 1 :
            ]
            with open(params.project_path, "wb", encoding="utf-8") as f:
                shutil.copyfileobj(response.raw, f)
                logging.info("Downloaded successfully.")

        if params.project_path.endswith(".tar.gz"):
            with tarfile.open(params.project_path, "r:gz", encoding="utf-8") as tar:
                tar.extractall(os.path.dirname(params.project_path))
            params.project_path = params.project_path[:-7]
        elif params.project_path.endswith(".tar"):
            with tarfile.open(params.project_path, "r:", encoding="utf-8") as tar:
                tar.extractall(os.path.dirname(params.project_path))
            params.project_path = params.project_path[:-4]

        importer = Importer(params.host_upload, params.project_path, **options)
        importer.run()
