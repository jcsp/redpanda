rp_test(
  FIXTURE_TEST
  BINARY_NAME archival_service
  SOURCES service_fixture.cc segment_reupload_test.cc ntp_archiver_test.cc service_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES v::seastar_testing_main v::application Boost::unit_test_framework v::archival v::storage_test_utils v::cloud_roles
  ARGS "-- -c 1"
  LABELS archival
)
