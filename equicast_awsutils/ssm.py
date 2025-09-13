from dataclasses import dataclass
from typing import Optional

import boto3
from botocore.exceptions import ClientError


@dataclass
class SSM:
    region_name: str
    session: Optional[boto3.Session] = None

    _parameter_types = ["String", "StringList", "SecureString"]

    def __post_init__(self):
        if not self.session:
            self.session = boto3.Session(region_name=self.region_name)

        self.client = self.session.client("ssm", region_name=self.region_name)

    def update_parameter(self, param_name: str, value: str, type_: str = "String", overwrite: bool = True):
        if type_ not in self._parameter_types:
            raise ValueError(f"Invalid parameter type: {type_}")

        try:
            self.client.put_parameter(
                Name=param_name,
                Value=value,
                Type=type_,
                Overwrite=overwrite
            )
            print(f"Updated parameter {param_name} to {value}")
            return True
        except ClientError as e:
            print(f"Failed to update parameter {param_name}: {e}")
            return False
