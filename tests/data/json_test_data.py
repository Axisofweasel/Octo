test_url = 'http://someurl.com/test.json'


octo_account_response = {
    "number": "ABCD123",
    "properties": [
        {
            "id": 1234567,
            "moved_in_at": "2020-11-30T00:00:00Z",
            "moved_out_at": 'null',
            "address_line_1": "10 Downing Street",
            "address_line_2": "",
            "address_line_3": "",
            "town": "LONDON",
            "county": "",
            "postcode": "W1 1AA",
            "electricity_meter_points": [
                {
                    "mpan": "1000000000000",
                    "profile_class": 1,
                    "consumption_standard": 2560,
                    "meters": [
                        {
                            "serial_number": "1111111111",
                            "registers": [
                                {
                                    "identifier": "1",
                                    "rate": "STANDARD",
                                    "is_settlement_register": 'true',
                                }
                            ],
                        },
                        {
                            "serial_number": "2222222222",
                            "registers": [
                                {
                                    "identifier": "1",
                                    "rate": "STANDARD",
                                    "is_settlement_register": 'true',
                                }
                            ],
                        },
                    ],
                    "agreements": [
                        {
                            "tariff_code": "E-1R-VAR-20-09-22-N",
                            "valid_from": "2020-12-17T00:00:00Z",
                            "valid_to": "2021-12-17T00:00:00Z",
                        },
                        {
                            "tariff_code": "E-1R-VAR-21-09-29-N",
                            "valid_from": "2021-12-17T00:00:00Z",
                            "valid_to": "2023-04-01T00:00:00+01:00",
                        },
                        {
                            "tariff_code": "E-1R-VAR-22-11-01-N",
                            "valid_from": "2023-04-01T00:00:00+01:00",
                            "valid_to": 'null',
                        },
                    ],
                    "is_export": 'False',
                }
            ],
            "gas_meter_points": [
                {
                    "mprn": "1234567890",
                    "consumption_standard": 3448,
                    "meters": [
                        {"serial_number": "12345678901234"},
                        {"serial_number": "09876543210987"},
                    ],
                    "agreements": [
                        {
                            "tariff_code": "G-1R-VAR-20-09-22-N",
                            "valid_from": "2020-12-17T00:00:00Z",
                            "valid_to": "2021-12-17T00:00:00Z",
                        },
                        {
                            "tariff_code": "G-1R-VAR-21-09-29-N",
                            "valid_from": "2021-12-17T00:00:00Z",
                            "valid_to": "2023-04-01T00:00:00+01:00",
                        },
                        {
                            "tariff_code": "G-1R-VAR-22-11-01-N",
                            "valid_from": "2023-04-01T00:00:00+01:00",
                            "valid_to": 'null',
                        },
                    ],
                }
            ],
        }
    ],
}

octo_consumption_response = {
    "count": 3,
    "next": 'https://api.octopus.energy/',
    "previous": 'https://api.octopus.energy/',
    "results": [
        {
        "consumption": 0.045,
        "interval_start": "2023-03-26T00:00:00Z",
        "interval_end": "2023-03-26T00:30:00Z"
    },
    {
        "consumption": 0.078,
        "interval_start": "2023-03-26T00:30:00Z",
        "interval_end": "2023-03-26T02:00:00+01:00"
    },
    {
        "consumption": 0.082,
        "interval_start": "2023-03-26T02:00:00+01:00",
        "interval_end": "2023-03-26T02:30:00+01:00"
    }
    ]
}