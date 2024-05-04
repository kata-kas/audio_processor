package security

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
)

type TokenData struct {
	Iat          string `json:"iat"`
	Admin        bool   `json:"admin"`
	Organization string `json:"organization"`
	Username     string `json:"username"`
	UserID       int    `json:"user_id"`
	IsSubscribed bool   `json:"is_subscribed"`
}

var tokenCache = make(map[string]*TokenData)

func GetTokenData(token string) (*TokenData, error) {
	data, found := tokenCache[token]
	if found && data.IsSubscribed {
		return data, nil
	}

	req, err := http.NewRequest("POST", os.Getenv("tokenDataAPIURL"), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set(os.Getenv("SECURITY_HEADER"), token)

	client := &http.Client{}
	response, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	if response.StatusCode == http.StatusOK {
		var responseData *TokenData
		if err := json.NewDecoder(response.Body).Decode(&responseData); err != nil {
			return nil, err
		}
		tokenCache[token] = responseData
		return responseData, nil
	} else {
		return nil, fmt.Errorf("request failed with status code %d: %s", response.StatusCode, response.Status)
	}
}
