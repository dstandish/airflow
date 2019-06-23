# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import datetime
import pendulum
import unittest
from importlib import reload

from airflow.utils import timezone

CET = pendulum.timezone("Europe/Paris")
EAT = pendulum.timezone('Africa/Nairobi')  # Africa/Nairobi
ICT = pendulum.timezone('Asia/Bangkok')  # Asia/Bangkok
UTC = timezone.utc


# CEST_CET_CUSP_DT_ARGS represents cusp of CEST -> CET in Europe/Paris
# CEST_CET_CUSP_DT_ARGS CET (+01:00) if POST_TRANSITION, and CEST (+02:00) if PRE_TRANSITION
CEST_CET_CUSP_DT_ARGS = [2018, 10, 28, 2, 55]


class TimezoneTest(unittest.TestCase):
    def tearDown(self) -> None:
        reload(timezone)

    def test_is_aware(self):
        self.assertTrue(timezone.is_localized(datetime.datetime(2011, 9, 1, 13, 20, 30, tzinfo=EAT)))
        self.assertFalse(timezone.is_localized(datetime.datetime(2011, 9, 1, 13, 20, 30)))

    def test_is_naive(self):
        self.assertFalse(timezone.is_naive(datetime.datetime(2011, 9, 1, 13, 20, 30, tzinfo=EAT)))
        self.assertTrue(timezone.is_naive(datetime.datetime(2011, 9, 1, 13, 20, 30)))

    def test_utcnow(self):
        now = timezone.utcnow()
        self.assertTrue(timezone.is_localized(now))
        self.assertEqual(now.replace(tzinfo=None), now.astimezone(UTC).replace(tzinfo=None))

    def test_convert_to_utc(self):
        naive = datetime.datetime(2011, 9, 1, 13, 20, 30)
        utc = datetime.datetime(2011, 9, 1, 13, 20, 30, tzinfo=UTC)
        self.assertEqual(utc, timezone.convert_to_utc(naive))

        eat = datetime.datetime(2011, 9, 1, 13, 20, 30, tzinfo=EAT)
        utc = datetime.datetime(2011, 9, 1, 10, 20, 30, tzinfo=UTC)
        self.assertEqual(utc, timezone.convert_to_utc(eat))

    def test_convert_to_utc_cusp(self):
        # Test behavior with ambiguous time, i.e. during DST transition.

        # aware datetime cusp
        dt = datetime.datetime(*CEST_CET_CUSP_DT_ARGS, tzinfo=CET)
        self.assertEqual(
            '2018-10-28T01:55:00+00:00',
            timezone.convert_to_utc(dt).isoformat(),
            'aware cusp datetime -> utc'
        )

        # aware Pendulum cusp
        dt_p = pendulum.datetime(*CEST_CET_CUSP_DT_ARGS, tzinfo=CET)
        self.assertEqual(
            '2018-10-28T01:55:00+00:00',
            timezone.convert_to_utc(dt_p).isoformat(),
            'aware cusp Pendulum -> utc'
        )

    def test_convert_to_utc_cusp_no_timezone(self):
        dt_naive = datetime.datetime(*CEST_CET_CUSP_DT_ARGS)
        self.assertEqual(
            '2018-10-28T02:55:00+00:00',
            timezone.convert_to_utc(dt_naive).isoformat(),
            'naive cusp datetime -> utc, TIMEZONE=default'
        )

    def test_convert_to_utc_cusp_no_timezone_default_cusp(self):
        dt_naive = datetime.datetime(*CEST_CET_CUSP_DT_ARGS)
        timezone.TIMEZONE = CET
        self.assertEqual(
            '2018-10-28T01:55:00+00:00',
            timezone.convert_to_utc(dt_naive).isoformat(),
            'naive cusp datetime -> utc, TIMEZONE=Europe/Paris'
        )

        # note: the teardown method reloads the timezone module, which will reset timezone.TIMEZONE to default

    def test_make_naive(self):
        self.assertEqual(
            timezone.make_naive(datetime.datetime(2011, 9, 1, 13, 20, 30, tzinfo=EAT), EAT),
            datetime.datetime(2011, 9, 1, 13, 20, 30))
        self.assertEqual(
            timezone.make_naive(datetime.datetime(2011, 9, 1, 17, 20, 30, tzinfo=ICT), EAT),
            datetime.datetime(2011, 9, 1, 13, 20, 30))

        with self.assertRaises(ValueError):
            timezone.make_naive(datetime.datetime(2011, 9, 1, 13, 20, 30), EAT)

    def test_make_aware(self):
        self.assertEqual(
            timezone.make_aware(datetime.datetime(2011, 9, 1, 13, 20, 30), EAT),
            datetime.datetime(2011, 9, 1, 13, 20, 30, tzinfo=EAT))
        with self.assertRaises(ValueError):
            timezone.make_aware(datetime.datetime(2011, 9, 1, 13, 20, 30, tzinfo=EAT), EAT)

    def test_make_aware_cusp(self):
        naive_dt = datetime.datetime(*CEST_CET_CUSP_DT_ARGS)
        # this tz is CET (+01:00) if POST_TRANSITION, and CEST (+02:00) if PRE_TRANSITION
        self.assertEqual(
            '2018-10-28T02:55:00+01:00',
            timezone.make_aware(naive_dt, CET).isoformat(),
            'ambiguous datetime time resolves as with pendulum.POST_TRANSITION',
        )

    def test_make_aware_cusp_no_timezone(self):
        naive_dt = datetime.datetime(*CEST_CET_CUSP_DT_ARGS)
        self.assertEqual(
            '2018-10-28T02:55:00+00:00',
            timezone.make_aware(naive_dt).isoformat(),
            'no timezone given and default is UTC',
        )

    def test_make_aware_cusp_no_timezone_default_cusp(self):
        naive_dt = datetime.datetime(*CEST_CET_CUSP_DT_ARGS)
        # is CET (+01:00) if POST_TRANSITION, and CEST (+02:00) if PRE_TRANSITION
        timezone.TIMEZONE = CET
        self.assertEqual(
            '2018-10-28T02:55:00+01:00',
            timezone.make_aware(naive_dt).isoformat(),
            'no timezone given and default tz is Europe/Paris i.e. cusp datetime',
        )

        # Test behavior with ambiguous time, i.e. during DST transition.
        local_tz = pendulum.timezone('Europe/Zurich')
        naive_dt = datetime.datetime(2018, 10, 28, 2, 55)
        # this tz is CET (+01:00) if POST_TRANSITION, and CEST (+02:00) if PRE_TRANSITION
        self.assertEqual(
            '2018-10-28T02:55:00+01:00',
            timezone.make_aware(naive_dt, local_tz).isoformat(),
            'ambiguous datetime time resolves as with pendulum.POST_TRANSITION',
        )
