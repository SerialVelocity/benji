from functools import partial
from typing import Dict, Any, Optional

import kopf
import pykube
from apscheduler.jobstores.base import JobLookupError
from apscheduler.triggers.cron import CronTrigger

from benji.amqp import AMQPRPCClient
from benji.k8s_operator import apscheduler, kubernetes_client
from benji.k8s_operator.constants import LABEL_PARENT_KIND, API_VERSION, API_GROUP
from benji.k8s_operator.resources import track_job_status, delete_all_dependant_jobs, BenjiJob, APIObject, \
    NamespacedAPIObject
from benji.k8s_operator.utils import cr_to_job_name, build_version_labels_rbd, determine_rbd_image_location, \
    random_string

K8S_BACKUP_SCHEDULE_SPEC_SCHEDULE = 'schedule'
K8S_BACKUP_SCHEDULE_SPEC_PERSISTENT_VOLUME_CLAIM_SELECTOR = 'persistentVolumeClaimSelector'
K8S_BACKUP_SCHEDULE_SPEC_PERSISTENT_VOLUME_CLAIM_SELECTOR_MATCH_LABELS = 'matchLabels'
K8S_BACKUP_SCHEDULE_SPEC_PERSISTENT_VOLUME_CLAIM_SELECTOR_MATCH_NAMESPACE_LABELS = 'matchNamespaceLabels'


class BenjiBackupSchedule(NamespacedAPIObject):

    version = f'{API_GROUP}/{API_VERSION}'
    endpoint = 'benjibackupschedules'
    kind = 'BenjiBackupSchedule'


class ClusterBenjiBackupSchedule(APIObject):

    version = f'{API_GROUP}/{API_VERSION}'
    endpoint = 'clusterbenjibackupschedules'
    kind = 'ClusterBenjiBackupSchedule'


def backup_scheduler_job(*,
                         namespace_label_selector: str = None,
                         namespace: str = None,
                         label_selector: str,
                         parent_body,
                         logger):
    if namespace_label_selector is not None:
        namespaces = [
            namespace.metadata.name for namespace in pykube.Namespace.objects(kubernetes_client).filter(label_selector=namespace_label_selector)
        ]
    else:
        namespaces = [namespace]

    pvcs = []
    for ns in namespaces:
        pvcs.extend([
            o.obj
            for o in pykube.PersistentVolumeClaim().objects(kubernetes_client).filter(namespace=ns,
                                                                                      label_selector=label_selector)
        ])

    if len(pvcs) == 0:
        logger.warning(f'No PVC matched the selector {label_selector} in namespace(s) {", ".join(namespaces)}.')
        return

    rpc_client = AMQPRPCClient(queue='')
    for pvc in pvcs:
        if 'volumeName' not in pvc['spec'] or pvc['spec']['volumeName'] in (None, ''):
            continue

        version_uid = '{}-{}'.format(f'{pvc.metadata.namespace}-{pvc.metadata.name}'[:246], random_string(6))
        volume = '{}/{}'.format(pvc['metadata']['namespace'], pvc['metadata']['name'])
        pv = pykube.PersistentVolume().objects(kubernetes_client).get_by_name(pvc['spec']['volumeName'])
        pool, image = determine_rbd_image_location(pv)
        version_labels = build_version_labels_rbd(pvc, pv, pool, image)

        rpc_client.call_async('ceph_v1_backup',
                              version_uid=version_uid,
                              volume=volume,
                              pool=pool,
                              image=image,
                              version_labels=version_labels)
    rpc_client.call_async('terminate')

    command = ['benji-api-server', '--queue', rpc_client.queue]
    job = BenjiJob(kubernetes_client, command=command, parent_body=parent_body)
    job.create()


@kopf.on.resume(*BenjiBackupSchedule.group_version_plural())
@kopf.on.create(*BenjiBackupSchedule.group_version_plural())
@kopf.on.update(*BenjiBackupSchedule.group_version_plural())
@kopf.on.resume(*ClusterBenjiBackupSchedule.group_version_plural())
@kopf.on.create(*ClusterBenjiBackupSchedule.group_version_plural())
@kopf.on.update(*ClusterBenjiBackupSchedule.group_version_plural())
def benji_backup_schedule(namespace: str, spec: Dict[str, Any], body: Dict[str, Any], logger,
                          **_) -> Optional[Dict[str, Any]]:
    schedule = spec[K8S_BACKUP_SCHEDULE_SPEC_SCHEDULE]
    label_selector = spec[K8S_BACKUP_SCHEDULE_SPEC_PERSISTENT_VOLUME_CLAIM_SELECTOR].get(
        K8S_BACKUP_SCHEDULE_SPEC_PERSISTENT_VOLUME_CLAIM_SELECTOR_MATCH_LABELS, None)
    namespace_label_selector = None
    if body['kind'] == BenjiBackupSchedule.kind:
        namespace_label_selector = spec[K8S_BACKUP_SCHEDULE_SPEC_PERSISTENT_VOLUME_CLAIM_SELECTOR].get(
            K8S_BACKUP_SCHEDULE_SPEC_PERSISTENT_VOLUME_CLAIM_SELECTOR_MATCH_NAMESPACE_LABELS, None)

    job_name = cr_to_job_name(body, 'scheduler')
    apscheduler.add_job(partial(backup_scheduler_job,
                                namespace_label_selector=namespace_label_selector,
                                namespace=namespace,
                                label_selector=label_selector,
                                parent_body=body,
                                logger=logger),
                        CronTrigger.from_crontab(schedule),
                        name=job_name,
                        id=job_name,
                        replace_existing=True)


@kopf.on.delete(*BenjiBackupSchedule.group_version_plural())
@kopf.on.delete(*ClusterBenjiBackupSchedule.group_version_plural())
def benji_backup_schedule_delete(name: str, namespace: str, body: Dict[str, Any], logger,
                                 **_) -> Optional[Dict[str, Any]]:
    try:
        apscheduler.remove_job(job_id=cr_to_job_name(body, 'scheduler'))
    except JobLookupError:
        pass
    delete_all_dependant_jobs(name=name, namespace=namespace, kind=body['kind'], logger=logger)


@kopf.on.create('batch', 'v1', 'jobs', labels={LABEL_PARENT_KIND: BenjiBackupSchedule.kind})
@kopf.on.resume('batch', 'v1', 'jobs', labels={LABEL_PARENT_KIND: BenjiBackupSchedule.kind})
@kopf.on.delete('batch', 'v1', 'jobs', labels={LABEL_PARENT_KIND: BenjiBackupSchedule.kind})
@kopf.on.field('batch', 'v1', 'jobs', field='status', labels={LABEL_PARENT_KIND: BenjiBackupSchedule.kind})
def benji_track_job_status_backup_schedule(**kwargs) -> Optional[Dict[str, Any]]:
    return track_job_status(crd=BenjiBackupSchedule, **kwargs)


@kopf.on.create('batch', 'v1', 'jobs', labels={LABEL_PARENT_KIND: ClusterBenjiBackupSchedule.kind})
@kopf.on.resume('batch', 'v1', 'jobs', labels={LABEL_PARENT_KIND: ClusterBenjiBackupSchedule.kind})
@kopf.on.delete('batch', 'v1', 'jobs', labels={LABEL_PARENT_KIND: ClusterBenjiBackupSchedule.kind})
@kopf.on.field('batch', 'v1', 'jobs', field='status', labels={LABEL_PARENT_KIND: ClusterBenjiBackupSchedule.kind})
def benji_track_job_status_cluster_backup_schedule(**kwargs) -> Optional[Dict[str, Any]]:
    return track_job_status(crd=ClusterBenjiBackupSchedule, **kwargs)


@kopf.timer(*BenjiBackupSchedule.group_version_plural(), initial_delay=60, interval=60)
@kopf.timer(*ClusterBenjiBackupSchedule.group_version_plural(), initial_delay=60, interval=60)
def benji_backup_schedule_job_gc(name: str, namespace: str):
    pass
