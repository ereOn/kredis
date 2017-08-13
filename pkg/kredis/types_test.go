package kredis

import (
	"reflect"
	"testing"
)

func TestParseRedisInstance(t *testing.T) {
	testCases := []struct {
		S              string
		Expected       *RedisInstance
		ExpectedString string
	}{
		{
			"",
			nil,
			"",
		},
		{
			"myredis-0-1",
			&RedisInstance{
				Hostname: "myredis-0-1",
				Port:     "6379",
			},
			"myredis-0-1:6379",
		},
		{
			"myredis-0-1:6380",
			&RedisInstance{
				Hostname: "myredis-0-1",
				Port:     "6380",
			},
			"myredis-0-1:6380",
		},
		{
			"myredis-0-1:6380:bug",
			nil,
			"",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.S, func(t *testing.T) {
			value, err := ParseRedisInstance(testCase.S)

			if testCase.Expected == nil {
				if err == nil {
					t.Fatal("expected an error")
				}
			} else {
				if err != nil {
					t.Errorf("expected no error but got: %s", err)
				}

				if *testCase.Expected != value {
					t.Errorf("expected:\n%v\ngot:\n%v", *testCase.Expected, value)
				}

				valueStr := value.String()

				if valueStr != testCase.ExpectedString {
					t.Errorf("expected:\n%s\ngot:\n%s", testCase.ExpectedString, valueStr)
				}
			}
		})
	}
}
func TestParseMasterGroup(t *testing.T) {
	testCases := []struct {
		S              string
		Expected       MasterGroup
		ExpectedString string
	}{
		{
			"",
			MasterGroup{},
			"",
		},
		{
			"myredis-0-1",
			MasterGroup{
				{
					Hostname: "myredis-0-1",
					Port:     "6379",
				},
			},
			"myredis-0-1:6379",
		},
		{
			"myredis-0-1:6380,myredis-0-2",
			MasterGroup{
				{
					Hostname: "myredis-0-1",
					Port:     "6380",
				},
				{
					Hostname: "myredis-0-2",
					Port:     "6379",
				},
			},
			"myredis-0-1:6380,myredis-0-2:6379",
		},
		{
			"myredis-0-1:6380:bug",
			nil,
			"",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.S, func(t *testing.T) {
			value, err := ParseMasterGroup(testCase.S)

			if testCase.Expected == nil {
				if err == nil {
					t.Fatal("expected an error")
				}
			} else {
				if err != nil {
					t.Errorf("expected no error but got: %s", err)
				}

				if !reflect.DeepEqual(testCase.Expected, value) {
					t.Errorf("expected:\n%v\ngot:\n%v", testCase.Expected, value)
				}

				valueStr := value.String()

				if valueStr != testCase.ExpectedString {
					t.Errorf("expected:\n%s\ngot:\n%s", testCase.ExpectedString, valueStr)
				}
			}
		})
	}
}
