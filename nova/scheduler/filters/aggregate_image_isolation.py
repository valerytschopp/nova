# Copyright (c) 2018 SWITCH
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

from oslo_log import log as logging


import nova.conf
from nova.i18n import _LW
from nova.scheduler import filters
from nova.scheduler.filters import utils

CONF = nova.conf.CONF

LOG = logging.getLogger(__name__)


class AggregateImageIsolation(filters.BaseHostFilter):
    """Images with the property 'isolation_aggregate' value will be scheduled
    only on the host aggregate with the matching metadata 'isolation_aggregate'
    value.
    """

    # Aggregate data and instance type does not change within a request
    run_filter_once_per_request = True

    RUN_ON_REBUILD = True

    IMAGE_ISOLATION_PROPERTY = 'isolation_aggregate'

    def host_passes(self, host_state, spec_obj):
        """Checks a host in an aggregate that metadata value match
        with the image isolation property.
        """
        image_props = spec_obj.image.properties if spec_obj.image else {}
        try:
            image_isolation = image_props.get(self.IMAGE_ISOLATION_PROPERTY)
        except AttributeError:
            image_isolation = None

        host_metadata = utils.aggregate_metadata_get_by_host(host_state)
        host_isolations = host_metadata.get(self.IMAGE_ISOLATION_PROPERTY, None)

        if not image_isolation:
            return True

        # image have isolation property
        if (not host_isolations or
            image_isolation not in host_isolations):
            # no host/aggregate isolation property
            # or image_isolation is not in the host_isolations
            LOG.debug("%(host_state)s fails image isolation. "
                      "Metadata %(prop)s does not exist, "
                      "or does not match %(isolation)s.",
                      {'host_state': host_state,
                             'prop': self.IMAGE_ISOLATION_PROPERTY,
                        'isolation': image_isolation})
            return False

        return True
