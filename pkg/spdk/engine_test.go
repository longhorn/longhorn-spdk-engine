package spdk

import (
	"fmt"

	"github.com/sirupsen/logrus"

	commonbitmap "github.com/longhorn/go-common-libs/bitmap"

	. "gopkg.in/check.v1"
)

func (s *TestSuite) TestCheckInitiatorAndTargetCreationRequirements(c *C) {
	testCases := []struct {
		name                              string
		podIP                             string
		initiatorIP                       string
		targetIP                          string
		port                              int32
		targetPort                        int32
		standbyTargetPort                 int32
		expectedInitiatorCreationRequired bool
		expectedTargetCreationRequired    bool
		expectedError                     error
	}{
		{
			name:                              "Create both initiator and target instances",
			podIP:                             "192.168.1.1",
			initiatorIP:                       "192.168.1.1",
			targetIP:                          "192.168.1.1",
			port:                              0,
			targetPort:                        0,
			standbyTargetPort:                 0,
			expectedInitiatorCreationRequired: true,
			expectedTargetCreationRequired:    true,
			expectedError:                     nil,
		},
		{
			name:                              "Create local target instance on the node with initiator instance",
			podIP:                             "192.168.1.1",
			initiatorIP:                       "192.168.1.1",
			targetIP:                          "192.168.1.1",
			port:                              8080,
			targetPort:                        0,
			standbyTargetPort:                 0,
			expectedInitiatorCreationRequired: false,
			expectedTargetCreationRequired:    true,
			expectedError:                     nil,
		},
		{
			name:                              "Create local initiator instance only",
			podIP:                             "192.168.1.1",
			initiatorIP:                       "192.168.1.1",
			targetIP:                          "192.168.1.2",
			port:                              0,
			targetPort:                        0,
			standbyTargetPort:                 0,
			expectedInitiatorCreationRequired: true,
			expectedTargetCreationRequired:    false,
			expectedError:                     nil,
		},
		{
			name:                              "Create local target instance on the node without initiator instance",
			podIP:                             "192.168.1.2",
			initiatorIP:                       "192.168.1.1",
			targetIP:                          "192.168.1.2",
			port:                              0,
			targetPort:                        0,
			standbyTargetPort:                 0,
			expectedInitiatorCreationRequired: false,
			expectedTargetCreationRequired:    true,
			expectedError:                     nil,
		},
		{
			name:                              "Invalid initiator and target addresses",
			podIP:                             "192.168.1.1",
			initiatorIP:                       "192.168.1.2",
			targetIP:                          "192.168.1.3",
			port:                              0,
			targetPort:                        0,
			standbyTargetPort:                 0,
			expectedInitiatorCreationRequired: false,
			expectedTargetCreationRequired:    false,
			expectedError:                     fmt.Errorf("invalid initiator and target addresses for engine test-engine creation with initiator address 192.168.1.2 and target address 192.168.1.3"),
		},
		{
			name:                              "Standby target instance is already created",
			podIP:                             "192.168.1.1",
			initiatorIP:                       "192.168.1.1",
			targetIP:                          "192.168.1.1",
			port:                              100,
			targetPort:                        0,
			standbyTargetPort:                 105,
			expectedInitiatorCreationRequired: false,
			expectedTargetCreationRequired:    false,
			expectedError:                     nil,
		},
	}
	for testName, testCase := range testCases {
		c.Logf("testing checkInitiatorAndTargetCreationRequirements.%v", testName)

		engine := &Engine{
			Port:              testCase.port,
			TargetPort:        testCase.targetPort,
			StandbyTargetPort: testCase.standbyTargetPort,
			Name:              "test-engine",
			log:               logrus.New(),
		}

		initiatorCreationRequired, targetCreationRequired, err := engine.checkInitiatorAndTargetCreationRequirements(testCase.podIP, testCase.initiatorIP, testCase.targetIP)

		c.Assert(initiatorCreationRequired, Equals, testCase.expectedInitiatorCreationRequired,
			Commentf("Test case '%s': unexpected initiator creation requirement", testCase.name))
		c.Assert(targetCreationRequired, Equals, testCase.expectedTargetCreationRequired,
			Commentf("Test case '%s': unexpected target creation requirement", testCase.name))
		c.Assert(err, DeepEquals, testCase.expectedError,
			Commentf("Test case '%s': unexpected error result", testCase.name))
	}
}

func (s *TestSuite) TestIsNewEngine(c *C) {
	testCases := []struct {
		name     string
		engine   *Engine
		expected bool
	}{
		{
			name: "New engine with empty IP and TargetIP and StandbyTargetPort 0",
			engine: &Engine{
				IP:                "",
				TargetIP:          "",
				StandbyTargetPort: 0,
			},
			expected: true,
		},
		{
			name: "Engine with non-empty IP",
			engine: &Engine{
				IP:                "192.168.1.1",
				TargetIP:          "",
				StandbyTargetPort: 0,
			},
			expected: false,
		},
		{
			name: "Engine with non-empty TargetIP",
			engine: &Engine{
				IP:                "",
				TargetIP:          "192.168.1.2",
				StandbyTargetPort: 0,
			},
			expected: false,
		},
		{
			name: "Engine with non-zero StandbyTargetPort",
			engine: &Engine{
				IP:                "",
				TargetIP:          "",
				StandbyTargetPort: 8080,
			},
			expected: false,
		},
	}

	for testName, testCase := range testCases {
		c.Logf("testing isNewEngine.%v", testName)
		result := testCase.engine.isNewEngine()
		c.Assert(result, Equals, testCase.expected, Commentf("Test case '%s': unexpected result", testCase.name))
	}
}

func (s *TestSuite) TestReleaseTargetAndStandbyTargetPorts(c *C) {
	testCases := []struct {
		name                      string
		engine                    *Engine
		expectedTargetPort        int32
		expectedStandbyTargetPort int32
		expectedError             error
	}{
		{
			name: "Release both target and standby target ports",
			engine: &Engine{
				TargetPort:        2000,
				StandbyTargetPort: 2005,
			},
			expectedTargetPort:        0,
			expectedStandbyTargetPort: 0,
			expectedError:             nil,
		},
		{
			name: "Release target port only but standby target port is not set",
			engine: &Engine{
				TargetPort:        2000,
				StandbyTargetPort: 0,
			},
			expectedTargetPort:        0,
			expectedStandbyTargetPort: 0,
			expectedError:             nil,
		},
		{
			name: "Release target and standby ports when they are the same",
			engine: &Engine{
				TargetPort:        2000,
				StandbyTargetPort: 2000,
			},
			expectedTargetPort:        0,
			expectedStandbyTargetPort: 0,
			expectedError:             nil,
		},
		{
			name: "Release snapshot target port only",
			engine: &Engine{
				TargetPort:        0,
				StandbyTargetPort: 2000,
			},
			expectedTargetPort:        0,
			expectedStandbyTargetPort: 0,
			expectedError:             nil,
		},
	}

	for testName, testCase := range testCases {
		c.Logf("testing releaseTargetAndStandbyTargetPorts.%v", testName)

		bitmap, err := commonbitmap.NewBitmap(0, 100000)
		c.Assert(err, IsNil)

		err = testCase.engine.releaseTargetAndStandbyTargetPorts(bitmap)
		c.Assert(err, DeepEquals, testCase.expectedError, Commentf("Test case '%s': unexpected error result", testCase.name))
		c.Assert(testCase.engine.TargetPort, Equals, testCase.expectedTargetPort, Commentf("Test case '%s': unexpected target port", testCase.name))
		c.Assert(testCase.engine.StandbyTargetPort, Equals, testCase.expectedStandbyTargetPort, Commentf("Test case '%s': unexpected standby target port", testCase.name))
	}
}
