from nose.tools import assert_raises
from pyqs import events
from tests.utils import clear_events_registry


@clear_events_registry
def test_register_event():
    def print_pre_process(context):
        print(context)

    events.register_event("pre_process", print_pre_process)
    events.get_events().pre_process.should.equal([print_pre_process])


@clear_events_registry
def test_register_multiple_same_events():
    def print_pre_process(context):
        print(context)

    def print_numbers(context):
        print(1 + 2)

    events.register_event("pre_process", print_pre_process)
    events.register_event("pre_process", print_numbers)
    events.get_events().pre_process.should.equal([
        print_pre_process, print_numbers
    ])


@clear_events_registry
def test_register_different_events():
    def print_pre_process(context):
        print(context)

    def print_post_process(context):
        print(context)

    events.register_event("pre_process", print_pre_process)
    events.register_event("post_process", print_post_process)
    events.get_events().pre_process.should.equal([print_pre_process])
    events.get_events().post_process.should.equal([print_post_process])


@clear_events_registry
def test_register_multiple_different_events():
    def print_pre_process(context):
        print(context)

    def print_post_process(context):
        print(context)

    def print_numbers(context):
        print(1 + 2)

    events.register_event("pre_process", print_pre_process)
    events.register_event("pre_process", print_numbers)
    events.register_event("post_process", print_post_process)
    events.register_event("post_process", print_numbers)
    events.get_events().pre_process.should.equal([
        print_pre_process, print_numbers
    ])
    events.get_events().post_process.should.equal([
        print_post_process, print_numbers
    ])


@clear_events_registry
def test_register_non_existent_event():
    non_existent_event = "non_existent_event"
    assert_raises(
        events.NoEventException,
        events.register_event,
        non_existent_event,
        lambda x: x
    )
