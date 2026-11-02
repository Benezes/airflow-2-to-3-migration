from __future__ import annotations

from kubernetes.client import models as k8s


def pod_override(memory: str = "1Gi", cpu: str = "500m") -> dict:
    return {
        "pod_override": k8s.V1Pod(
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name="base",
                        resources=k8s.V1ResourceRequirements(
                            requests={"memory": "512Mi", "cpu": "250m"},
                            limits={"memory": memory, "cpu": cpu},
                        ),
                    )
                ]
            )
        )
    }
