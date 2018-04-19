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

from nova.scheduler import filters
from nova.scheduler.filters import utils

LOG = logging.getLogger(__name__)


class AggregateImageOsDistroIsolation(filters.BaseHostFilter):
    """Images with the property 'os_distro' value will be scheduled
    ONLY on the host aggregate with the matching metadata 'os_distro'
    value.
    Scheduling will fail if no host aggregate matches the image's
    'os_distro' property.
    """

    # Aggregate data and instance type do not change within a request
    run_filter_once_per_request = True

    RUN_ON_REBUILD = True

    def host_passes(self, host_state, spec_obj):
        """Checks that the host is in an aggregate whose metadata 'os_distro'
        value matches the image's 'os_distro' property.
        """
        ISOLATION_PROPERTY = 'os_distro'

        image_props = spec_obj.image.properties if spec_obj.image else {}
        try:
            image_isolation = image_props.get(ISOLATION_PROPERTY)
        except AttributeError:
            image_isolation = None

        host_metadata = utils.aggregate_metadata_get_by_host(host_state)
        host_isolations = host_metadata.get(ISOLATION_PROPERTY, None)

        if not image_isolation:
            # image without the 'os_distro' property can be scheduled
            # on any host
            return True

        # image has isolation property os_distro
        if not host_isolations or image_isolation not in host_isolations:
            # no host/aggregate isolation metadata
            # or image_isolation is not in the host_isolations
            LOG.debug("%(host_state)s fails image os_distro isolation. "
                      "Host aggregate metadata %(prop)s does not exist, "
                      "or does not match image %(isolation)s.",
                      {'host_state': host_state,
                       'prop': ISOLATION_PROPERTY,
                       'isolation': image_isolation})
            return False

        return True
