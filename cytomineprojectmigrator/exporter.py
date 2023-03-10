"""Cytomine Project Exporter

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

import logging
import os
import shutil
import sys
from argparse import ArgumentParser
from datetime import datetime
from joblib import Parallel, delayed

from cytomine import Cytomine
from cytomine.models import (
    AnnotationCollection,
    AttachedFileCollection,
    Collection,
    Description,
    ImageGroupCollection,
    ImageGroupImageInstanceCollection,
    ImageInstanceCollection,
    Model,
    Ontology,
    Project,
    PropertyCollection,
    TermCollection,
    User,
    UserCollection,
)

__author__ = "Rubens Ulysse <urubens@uliege.be>"


def find_or_append_by_id(obj, l):
    """Find obj in a list if not found add it to the list"""
    items = [i for i in l if i.id == obj.id]
    if items and len(items) > 0:
        return items[0]

    l.append(obj)
    return obj


class Exporter:
    """Export a Cytomine Project as an archive from a Cytomine server"""

    def __init__(
        self,
        working_path,
        id_project,
        without_image_download=False,
        without_image_groups=False,
        without_user_annotations=False,
        without_metadata=False,
        without_annotation_metadata=False,
        anonymize=False,
    ):
        self.project = Project().fetch(id_project)
        if not self.project:
            raise ValueError("Project not found")

        items = [
            Cytomine.get_instance().host,
            self.project.id,
            self.project.name,
            datetime.now(),
        ]
        self.project_directory = "-".join(
            [str(item).replace(" ", "-") for item in items]
        )
        self.working_path = working_path
        self.project_path = os.path.join(working_path, self.project_directory)
        self.attached_file_path = None

        self.with_image_download = not without_image_download
        self.with_image_groups = not without_image_groups
        self.with_user_annotations = not without_user_annotations
        self.with_annotation_metadata = not without_annotation_metadata
        self.with_metadata = not without_metadata
        self.anonymize = anonymize

        self.users = UserCollection()

    def export_metadata(self, objects):
        """Export the medata"""

        def _export_metadata(save_object_fn, obj, attached_file_path):
            properties = PropertyCollection(obj).fetch()
            if len(properties) > 0:
                save_object_fn(properties, f"properties-object-{obj.id}-collection")

            attached_files = AttachedFileCollection(obj).fetch()
            if len(attached_files) > 0:
                save_object_fn(
                    attached_files, f"attached-files-object-{obj.id}-collection"
                )
                for attached_file in attached_files:
                    attached_file.download(
                        os.path.join(attached_file_path, "{filename}"), True
                    )

            description = Description(obj).fetch()
            if description:
                save_object_fn(description, f"description-object-{obj.id}")

        Parallel(n_jobs=-1, backend="threading")(
            delayed(_export_metadata)(self.save_object, obj, self.attached_file_path)
            for obj in objects
        )

    def save_user(self, user, role=None):
        """Save the user of the project"""

        u = find_or_append_by_id(user, self.users)
        if not hasattr(u, "roles"):
            u.roles = []
        if role:
            u.roles.append(role)

    def save_object(self, obj, filename=None):
        """Save the object"""

        if not obj:
            return

        if filename:
            filename = f"{filename}.json"
        elif isinstance(obj, Model):
            filename = f"{obj.callback_identifier}-{obj.id}.json"
        elif isinstance(obj, Collection):
            filename = f"{obj.callback_identifier}-collection.json"

        with open(
            os.path.join(self.project_path, filename), "w", encoding="utf-8"
        ) as outfile:
            outfile.write(obj.to_json())
            logging.info("Object %s has been saved locally.", obj)

    def export_project(self):
        """Export a Cytomine project"""

        self.save_object(self.project)

        logging.info("1.1/ Export project managers")
        admins = UserCollection(admin=True).fetch_with_filter(
            "project", self.project.id
        )
        for admin in admins:
            self.save_user(admin, "project_manager")

        logging.info("1.2/ Export project contributors")
        users = UserCollection().fetch_with_filter("project", self.project.id)
        for user in users:
            self.save_user(user, "project_contributor")

        if self.with_metadata:
            logging.info("1.3/ Export project metadata")
            self.export_metadata([self.project])

    def export_ontology(self):
        """Export an ontology"""

        ontology = Ontology().fetch(self.project.ontology)
        self.save_object(ontology)

        logging.info("2.1/ Export ontology creator")
        user = User().fetch(ontology.user)
        self.save_user(user, "ontology_creator")

        if self.with_metadata:
            logging.info("2.2/ Export ontology metadata")
            self.export_metadata([ontology])

    def export_terms(self):
        """Export terms"""

        terms = TermCollection().fetch_with_filter("project", self.project.id)
        self.save_object(terms)

        if self.with_metadata:
            logging.info("3.1/ Export term metadata")
            self.export_metadata(terms)

    def export_images(self):
        """Export images"""

        images = ImageInstanceCollection().fetch_with_filter("project", self.project.id)
        self.save_object(images)

        if self.with_image_download:
            image_path = os.path.join(self.project_path, "images")
            os.makedirs(image_path)

            def _download_image(image, path):
                logging.info("Download file for image %s", image)
                image.download(
                    os.path.join(path, image.originalFilename),
                    override=False,
                    parent=True,
                )

            # Temporary use threading as backend, as we need to connect to Cytomine in every other processes.
            Parallel(n_jobs=-1, backend="threading")(
                delayed(_download_image)(image, image_path) for image in images
            )

        logging.info("4.1/ Export image creator users")
        image_users = set(image.user for image in images)
        for image_user in image_users:
            user = User().fetch(image_user)
            self.save_user(user, "image_creator")

        logging.info("4.2/ Export image reviewer users")
        image_users = set(image.reviewUser for image in images if image.reviewUser)
        for image_user in image_users:
            user = User().fetch(image_user)
            self.save_user(user, "image_reviewer")

        if self.with_metadata:
            logging.info("4.3/ Export image metadata")
            self.export_metadata(images)

    def export_annotations(self):
        """Export user annotations"""

        user_annotations = AnnotationCollection(
            showWKT=True, showTerm=True, project=self.project.id
        ).fetch()
        self.save_object(user_annotations, filename="user-annotation-collection")

        logging.info("4.1/ Export user annotation creator users")
        annotation_users = set(annotation.user for annotation in user_annotations)
        for annotation_user in annotation_users:
            user = User().fetch(annotation_user)
            self.save_user(user, "userannotation_creator")

        logging.info("4.2/ Export user annotation term creator users")
        annotation_users = set()
        for annotation in user_annotations:
            annotation_users = annotation_users.union(
                *[
                    set(item["user"])
                    for item in annotation.userByTerm
                    if annotation.userByTerm
                ]
            )

        for annotation_user in annotation_users:
            user = User().fetch(annotation_user)
            self.save_user(user, "userannotationterm_creator")

        if self.with_annotation_metadata:
            logging.info("4.3/ Export user annotation metadata")
            self.export_metadata(user_annotations)

    def export_users(self):
        """Export users of the project"""

        if self.anonymize:
            for i, user in enumerate(self.users):
                user.username = f"anonymized_user{i + 1}"
                user.firstname = "Anonymized"
                user.lastname = f"User {i + 1}"
                user.email = f"anonymous{i + 1}@unknown.com"

        self.save_object(self.users)

        # Disabled due to core issue.
        # if self.with_metadata:
        #     logging.info("5.1/ Export user metadata")
        #     self.export_metadata(self.users)

    def export_image_groups(self):
        """Export image groups"""
        image_groups = ImageGroupCollection().fetch_with_filter(
            "project", self.project.id
        )
        self.save_object(image_groups)

        if self.with_metadata:
            logging.info("6.1/ Export image group metadata")
            self.export_metadata(image_groups)

        if self.with_image_download:
            image_group_path = os.path.join(self.project_path, "imagegroups")
            os.makedirs(image_group_path)
            for image_group in image_groups:
                image_group.download(
                    os.path.join(image_group_path, image_group.name),
                    override=False,
                    parent=True,
                )

        image_sequences = ImageGroupImageInstanceCollection()
        for image_group in image_groups:
            image_sequences += ImageGroupImageInstanceCollection().fetch_with_filter(
                "imagegroup", image_group.id
            )
        self.save_object(image_sequences)

    def run(self):
        """Serialize a Cytomine project"""

        logging.info("Export will be done in directory %s", self.project_path)
        os.makedirs(self.project_path)

        if self.with_metadata or self.with_annotation_metadata:
            self.attached_file_path = os.path.join(self.project_path, "attached_files")
            os.makedirs(self.attached_file_path)

        logging.info("1/ Export project %s", self.project.id)
        self.export_project()

        logging.info("2/ Export ontology %s", self.project.ontology)
        self.export_ontology()

        logging.info("3/ Export terms")
        self.export_terms()

        logging.info("4/ Export images")
        self.export_images()

        logging.info("4/ Export user annotations")
        self.export_annotations()

        logging.info("5/ Export users")
        self.export_users()

        if self.with_image_groups:
            logging.info("6/ Export image groups")
            self.export_image_groups()

        logging.info("Finished.")

    def make_archive(self):
        """Create an archive of the project"""

        logging.info("Making archive...")
        shutil.make_archive(
            self.project_path, "gztar", self.working_path, self.project_directory
        )
        logging.info("Finished.")


if __name__ == "__main__":
    parser = ArgumentParser(prog="Cytomine Project Exporter")
    parser.add_argument(
        "--host", help="The Cytomine host from which project is exported."
    )
    parser.add_argument(
        "--public_key",
        help="The Cytomine public key used to export the project. "
        "The underlying user has to be a manager of the exported project.",
    )
    parser.add_argument(
        "--private_key",
        help="The Cytomine private key used to export the project. "
        "The underlying user has to be a manager of the exported project.",
    )
    parser.add_argument(
        "--id_project", help="The Cytomine identifier of the project to export."
    )
    parser.add_argument(
        "--make_archive", default=True, help="Make an archive for the exported project."
    )
    parser.add_argument(
        "--working_path",
        default="",
        help="The base path where the generated archive will be stored.",
    )
    parser.add_argument(
        "--anonymize", default=False, help="Anonymize users in the project."
    )
    parser.add_argument(
        "--without_image_download",
        default=False,
        help="Do not download images but export image metadata.",
    )
    parser.add_argument(
        "--without_image_groups", default=False, help="Do not export image groups."
    )
    parser.add_argument(
        "--without_user_annotations",
        default=False,
        help="Do not export user annotations.",
    )
    parser.add_argument(
        "--without_metadata", default=False, help="Do not export any metadata."
    )
    parser.add_argument(
        "--without_annotation_metadata",
        default=True,
        help="Do not export annotation metadata " "(speed up processing).",
    )
    params, other = parser.parse_known_args(sys.argv[1:])

    with Cytomine(params.host, params.public_key, params.private_key) as _:
        Cytomine.get_instance().open_admin_session()
        options = {
            k: v
            for (k, v) in vars(params).items()
            if k.startswith("without") or k == "anonymize"
        }
        exporter = Exporter(params.working_path, params.id_project, **options)
        exporter.run()
        if params.make_archive:
            exporter.make_archive()
        Cytomine.get_instance().close_admin_session()
