import nose.tools as nt

from stolos import get_NS
from stolos import exceptions


def outputs_expected_return_values(app1, **kwargs):
    qbmodule = get_NS().queue_backend
    with nt.assert_raises(exceptions.NoNodeError):
        qbmodule.get_children(app1)
    qbmodule.create(app1, 'value', makepath=True)
    nt.assert_equal(qbmodule.get_children(app1, []))

    qbmodule.create(app1)
    nt.assert_equal(qbmodule.get_children(app1), [])
    raise NotImplementedError()
