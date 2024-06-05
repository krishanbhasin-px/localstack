import json
import logging
import os

import aws_cdk as cdk
import aws_cdk.aws_events as events
import aws_cdk.aws_events_targets as targets
import aws_cdk.aws_iam as iam
import aws_cdk.aws_lambda as _lambda
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_sqs as sqs
import pytest

from localstack.testing.pytest import markers
from localstack.utils.files import load_file
from tests.aws.services.events.helper_functions import sqs_collect_messages

LOG = logging.getLogger(__name__)

REGION_PRIMARY = "us-east-1"
REGION_SECONDARY = "eu-central-1"

STACK_NAME_PRIMARY = "EventsStackPrimaryRegion"
STACK_NAME_SECONDARY = "EventsStackSecondaryRegion"

S3_BUCKET_NAME = "eventbridge-secondary-s3-bucket"

EVENT_BUS_NAME_PRIMARY = "event_bus_primary"
EVENT_BUS_NAME_SECONDARY = "event_bus_secondary"

SOURCE_PRODUCER_PRIMARY_ONE = "producer-primary-one"
SOURCE_PRODUCER_PRIMARY_TWO = "producer-primary-two"
SOURCE_PRODUCER_SECONDARY = "producer-secondary"

DEFAULT_MESSAGE = {
    "command": "update-account",
    "payload": {"acc_id": "0a787ecb-4015", "sf_id": "baz"},
}


def _read_file_as_string(filename: str) -> str:
    file_path = os.path.join(os.path.dirname(__file__), filename)
    return load_file(file_path)


class TestEventsCrossAccountRegionScenario:
    @pytest.fixture(scope="class", autouse=True)
    def infrastructure(self, infrastructure_setup, account_id):
        infra = infrastructure_setup(
            namespace="MultiStackDeploymentEventsCrossAccountRegionScenario", force_synth=True
        )
        ##########################
        # Primary region / account
        ##########################

        stack_primary = cdk.Stack(
            infra.cdk_app,
            STACK_NAME_PRIMARY,
            # cross_region_references=True,
            env=cdk.Environment(region=REGION_PRIMARY),
        )

        # Producer 1 lambda microservice
        lambda_producer_primary_one = _lambda.Function(
            stack_primary,
            id="ProducerPrimaryOne",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="lambda_handler",
            code=_lambda.InlineCode(code=_read_file_as_string("./_lambda/producer_primary_one.py")),
        )

        # Producer 2 lambda microservice
        lambda_producer_primary_two = _lambda.Function(
            stack_primary,
            id="ProducerPrimaryTwo",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="lambda_handler",
            code=_lambda.InlineCode(code=_read_file_as_string("./_lambda/producer_primary_two.py")),
        )

        lambda_rule = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=["events:PutEvents"],
            resources=["arn:aws:events:*:*:event-bus/*"],  # [event_bus_primary.event_bus_arn],
        )
        lambda_producer_primary_one.add_to_role_policy(lambda_rule)
        lambda_producer_primary_two.add_to_role_policy(lambda_rule)

        # Define the event bus in the secondary region
        event_bus_secondary = events.EventBus.from_event_bus_arn(
            stack_primary,
            id="EventBusSecondary",
            event_bus_arn=f"arn:aws:events:{REGION_SECONDARY}:{account_id}:event-bus/{EVENT_BUS_NAME_SECONDARY}",
        )

        # Role to put events from event bus 1 to event bus 2 in region secondary
        role_event_bus_primary_to_secondary = iam.Role(
            stack_primary,
            id="EventBusPrimaryToSecondaryRole",
            assumed_by=iam.ServicePrincipal("events.amazonaws.com"),
            inline_policies={
                "PutEventsPolicy": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            actions=["events:PutEvents"],
                            effect=iam.Effect.ALLOW,
                            resources=[
                                "arn:aws:events:*:*:event-bus/*"
                            ],  # [event_bus_secondary.event_bus_arn],
                        )
                    ]
                )
            },
        )

        # Event bus 1 central bus in region main region
        event_bus_primary = events.EventBus(
            stack_primary, id="EventBusPrimary", event_bus_name=EVENT_BUS_NAME_PRIMARY
        )

        # Rule to send events from producer 1 event bus 1 to event bus 2
        rule_event_bus_secondary = events.Rule(
            stack_primary,
            id="RuleEventBusSecondary",
            event_bus=event_bus_primary,
            event_pattern={"source": [SOURCE_PRODUCER_PRIMARY_ONE]},
        )
        rule_event_bus_secondary.add_target(
            targets.EventBus(event_bus_secondary, role=role_event_bus_primary_to_secondary)
        )

        # Sqs queue as target for all events
        sqs_queue_primary = sqs.Queue(stack_primary, "PrimaryQueue")
        sqs_queue_primary.add_to_resource_policy(
            iam.PolicyStatement(
                actions=["sqs:SendMessage"],
                effect=iam.Effect.ALLOW,
                resources=[sqs_queue_primary.queue_arn],
                principals=[iam.ServicePrincipal("events.amazonaws.com")],
            )
        )

        # Rule to send all events from event bus 1 to sqs queue
        events.Rule(
            stack_primary,
            id="RuleSqs",
            event_bus=event_bus_primary,
            event_pattern={"source": [SOURCE_PRODUCER_PRIMARY_ONE, SOURCE_PRODUCER_PRIMARY_TWO]},
            targets=[targets.SqsQueue(sqs_queue_primary)],
        )

        ############################
        # Secondary region / account
        ############################

        # Deploy the stack in the secondary region / account
        stack_secondary = cdk.Stack(
            infra.cdk_app,
            STACK_NAME_SECONDARY,
            # cross_region_references=True,
            env=cdk.Environment(region=REGION_SECONDARY),
        )

        # Consumer lambda microservice
        lambda_consumer = _lambda.Function(
            stack_secondary,
            id="Consumer",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="lambda_handler",
            code=_lambda.InlineCode(code=_read_file_as_string("./_lambda/consumer.py")),
        )

        # Lambda consumer S3 bucket
        bucket_consumer = s3.Bucket(
            stack_secondary,
            id="S3BucketConsumer",
            bucket_name=S3_BUCKET_NAME,
        )
        role_lambda_consumer_to_s3_bucket = iam.Role(
            stack_secondary,
            id="LambdaConsumerS3BucketRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
        )
        bucket_consumer.grant_write(role_lambda_consumer_to_s3_bucket)

        # Test producer lambda microservice
        lambda_producer_secondary = _lambda.Function(
            stack_secondary,
            id="ProducerSecondary",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="lambda_handler",
            code=_lambda.InlineCode(code=_read_file_as_string("./_lambda/producer_secondary.py")),
        )
        lambda_rule = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=["events:PutEvents"],
            resources=["arn:aws:events:*:*:event-bus/*"],  # [event_bus_primary.event_bus_arn],
        )
        lambda_producer_secondary.add_to_role_policy(lambda_rule)

        # Event bus 2 subscriber bus in region secondary
        event_bus_secondary = events.EventBus(
            stack_secondary, id="EventBusSecondary", event_bus_name=EVENT_BUS_NAME_SECONDARY
        )

        # Event bridge rule to send events to the consumer lambda
        events.Rule(
            stack_secondary,
            "RuleLambdaConsumer",
            event_bus=event_bus_secondary,
            event_pattern={
                "source": [SOURCE_PRODUCER_PRIMARY_ONE],
            },
            targets=[targets.LambdaFunction(lambda_consumer)],
        )

        # Sqs queue as target for all events
        sqs_queue_secondary = sqs.Queue(stack_secondary, "SecondaryQueue")
        sqs_queue_secondary.add_to_resource_policy(
            iam.PolicyStatement(
                actions=["sqs:SendMessage"],
                effect=iam.Effect.ALLOW,
                resources=[sqs_queue_secondary.queue_arn],
                principals=[iam.ServicePrincipal("events.amazonaws.com")],
            )
        )
        # Rule to send all events from event bus 1 to sqs queue
        events.Rule(
            stack_secondary,
            id="RuleSqs",
            event_bus=event_bus_secondary,
            event_pattern={
                "source": [
                    SOURCE_PRODUCER_PRIMARY_ONE,
                    SOURCE_PRODUCER_PRIMARY_TWO,
                    SOURCE_PRODUCER_SECONDARY,
                ]
            },
            targets=[targets.SqsQueue(sqs_queue_secondary)],
        )

        #########
        # Outputs
        #########

        cdk.CfnOutput(
            stack_primary,
            "LambdaProducerPrimaryOneFunctionName",
            value=lambda_producer_primary_one.function_name,
        )
        cdk.CfnOutput(
            stack_primary,
            "LambdaProducerPrimaryTwoFunctionName",
            value=lambda_producer_primary_two.function_name,
        )
        cdk.CfnOutput(
            stack_secondary, "LambdaConsumerFunctionName", value=lambda_consumer.function_name
        )
        cdk.CfnOutput(
            stack_secondary,
            "LambdaProducerSecondaryFunctionName",
            value=lambda_producer_secondary.function_name,
        )
        cdk.CfnOutput(stack_primary, "SqSQueuePrimaryUrl", value=sqs_queue_primary.queue_url)
        cdk.CfnOutput(stack_secondary, "SqSQueueSecondaryUrl", value=sqs_queue_secondary.queue_url)

        with infra.provisioner(skip_teardown=True) as provisioner:
            yield provisioner

    @markers.aws.validated
    def test_cross_region(self, infrastructure, aws_client_factory, snapshot):
        outputs_stack_primary = infrastructure.get_stack_outputs(STACK_NAME_PRIMARY)
        outputs_stack_secondary = infrastructure.get_stack_outputs(STACK_NAME_SECONDARY)

        lambda_function_name_producer_primary_one = outputs_stack_primary[
            "LambdaProducerPrimaryOneFunctionName"
        ]
        lambda_function_name_producer_primary_two = outputs_stack_primary[
            "LambdaProducerPrimaryTwoFunctionName"
        ]
        # lambda_function_name_consumer = outputs_stack_secondary["LambdaConsumerFunctionName"]
        lambda_function_name_producer_secondary = outputs_stack_secondary[
            "LambdaProducerSecondaryFunctionName"
        ]
        queue_url_primary = outputs_stack_primary["SqSQueuePrimaryUrl"]
        queue_url_secondary = outputs_stack_secondary["SqSQueueSecondaryUrl"]

        # Create boto3 clients for both regions
        aws_client_primary_region = aws_client_factory(region_name=REGION_PRIMARY)
        lambda_client_primary_region = aws_client_primary_region._lambda
        sqs_client_primary_region = aws_client_primary_region.sqs

        aws_client_secondary_region = aws_client_factory(region_name=REGION_SECONDARY)
        lambda_client_secondary_region = aws_client_secondary_region._lambda
        sqs_client_secondary_region = aws_client_secondary_region.sqs

        # Invoke lambda producer primary one
        invoke_result_producer_primary_one = lambda_client_primary_region.invoke(
            FunctionName=lambda_function_name_producer_primary_one,
            Payload=json.dumps(DEFAULT_MESSAGE),
            InvocationType="RequestResponse",
        )
        assert invoke_result_producer_primary_one["StatusCode"] == 200

        # Invoke lambda producer secondary one
        invoke_result_producer_primary_one = lambda_client_primary_region.invoke(
            FunctionName=lambda_function_name_producer_primary_two,
            Payload=json.dumps(DEFAULT_MESSAGE),
            InvocationType="RequestResponse",
        )
        assert invoke_result_producer_primary_one["StatusCode"] == 200

        # Check sqs queue primary for both messages
        messages_primary = sqs_collect_messages(
            sqs_client=sqs_client_primary_region,
            queue_url=queue_url_primary,
            min_events=2,
        )
        for message in messages_primary:
            body = json.loads(message["Body"])
            assert (
                body["source"] == SOURCE_PRODUCER_PRIMARY_ONE
                or body["source"] == SOURCE_PRODUCER_PRIMARY_TWO
            )
            assert body["detail"] == DEFAULT_MESSAGE

        # Invoke lambda producer secondary
        invoke_result_producer_secondary = lambda_client_secondary_region.invoke(
            FunctionName=lambda_function_name_producer_secondary,
            Payload=json.dumps(DEFAULT_MESSAGE),
            InvocationType="RequestResponse",
        )
        assert invoke_result_producer_secondary["StatusCode"] == 200

        # Check sqs queue secondary for fist and third message
        messages_secondary = sqs_collect_messages(
            sqs_client=sqs_client_secondary_region,
            queue_url=queue_url_secondary,
            min_events=2,  # one from primary region producer one and one from secondary region producer
        )
        for message in messages_secondary:
            body = json.loads(message["Body"])
            assert (
                body["source"] == SOURCE_PRODUCER_PRIMARY_ONE
                or body["source"] == SOURCE_PRODUCER_SECONDARY
            )
            assert body["detail"] == DEFAULT_MESSAGE
