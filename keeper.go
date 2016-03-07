package main

import (
	"os"
	"github.com/nlopes/slack"
	"gopkg.in/mgo.v2"
	"encoding/json"
	"gopkg.in/mgo.v2/bson"
	"log"
	"time"
	"strconv"
)

type History struct {
	Id string `bson:"_id"`
	Latest string `bson:"latest"`
	Messages []slack.Message `bson:"messages"`
}

func addMessagesFromSlackHistory(historyCollection *mgo.Collection,	chatCollection *mgo.Collection, getHistory func(string, slack.HistoryParameters)(*slack.History, error)){
	var chatId map[string]string
	// iterate through each group's ID in MongoDB
	iter := chatCollection.Find(nil).Select(bson.M{"_id":1}).Iter()
	for iter.Next(&chatId){
		gId := chatId["_id"]
		messageHistory := new(History)
		// find current group's history record in the collection
		err :=	historyCollection.Find(bson.M{"_id":gId}).One(&messageHistory)
		if err != nil {
			// if there is not record for this group yet, initialize the history to read from the start of it's slack history
			if err == mgo.ErrNotFound {
				messageHistory.Id = gId
				messageHistory.Latest = "0"
				messageHistory.Messages = []slack.Message{}
			} else {
				panic(err)
			}
		}

		// initial values for start of pulling slack history for current group
		isFirstRun := true
		hasMore := true
		latestTime := strconv.FormatInt(time.Now().Unix(), 10)
		oldestTime := messageHistory.Latest
		// get the slack conversations tied to the group, continue to run in loop
		// while hasMore variable indicates more slack history objects to query and save
		for hasMore {
			params := slack.HistoryParameters{Count: 100, Latest: latestTime, Oldest: oldestTime}
			history, _ := getHistory(gId, params)
			jsonHistory, _ := json.Marshal(history)
			log.Print("--- History from Slack ---\r\n"+string(jsonHistory))

			// Add to Messages to collection
			info, err := historyCollection.UpsertId(gId,
				bson.M{"$addToSet": bson.M{"Messages": bson.M{"$each": history.Messages } } })
			if err != nil {
				panic(err)
			}
			jsonInfo, _ := json.Marshal(info)
			log.Print("--- Added Messages to history collection ---\r\n"+string(jsonInfo))

			// if this is the first iteration in the for loop assign the first message's
			// timestamp as the new Latest value to save for the current group's history
			if isFirstRun {
				var ts string
				if len(history.Messages) > 0 {
					ts = history.Messages[0].Timestamp
					info, err := historyCollection.UpsertId(gId,
						bson.M{"$set" : bson.M{"latest" : ts}})
					if err != nil {
						panic(err)
					}
					jsonInfo, _ := json.Marshal(info)
					log.Print("--- Updated Latest Timestamp ---\r\n"+string(jsonInfo))
				}
				isFirstRun = false
			}
			// update all values for next execution of loop
			hasMore = history.HasMore
			if len(history.Messages) > 0 {
				lastMessage := history.Messages[len(history.Messages) - 1]
				latestTime = lastMessage.Timestamp
			}
		}
	}
}

func updateGroupsFromSlack(db *mgo.Database, api *slack.Client, historyCollection *mgo.Collection) {
	groupsCollection := db.C("groups")
	// get latest list of groups from api
	groups, err := api.GetGroups(false)
	if err != nil {
		panic(err)
	}
	// loop through current list and update the collection in MongoDB
	for _, group := range groups {
		groupJson, err := json.Marshal(&group)
		var groupData map[string]interface{}
		if err := json.Unmarshal(groupJson, &groupData); err != nil {
			panic(err)
		}
		_, err = groupsCollection.Upsert(bson.M{"_id": group.ID}, groupData)
		if err != nil {
			panic(err)
		}
	}

	addMessagesFromSlackHistory(historyCollection, groupsCollection, api.GetGroupHistory)
}

func updateChannelsFromSlack(db *mgo.Database, api *slack.Client, historyCollection *mgo.Collection) {
	channelsCollection := db.C("channels")
	// get latest list of channels from api
	channels, err := api.GetChannels(false)
	if err != nil {
		panic(err)
	}
	// loop through current list and update the collection in MongoDB
	for _, channel := range channels {
		channelJson, err := json.Marshal(&channel)
		var channelData map[string]interface{}
		if err := json.Unmarshal(channelJson, &channelData); err != nil {
			panic(err)
		}
		_, err = channelsCollection.Upsert(bson.M{"_id": channel.ID}, channelData)
		if err != nil {
			panic(err)
		}
	}
	addMessagesFromSlackHistory(historyCollection, channelsCollection, api.GetChannelHistory)
}

func main() {
	session, err := mgo.Dial(os.Getenv("MONGODB_URL"))
	if err != nil {
		panic(err)
	}
	defer session.Close()
	db := session.DB("slack-history")

	api := slack.New(os.Getenv("SLACK_TOKEN"))
	historyCollection := db.C("history")

	updateGroupsFromSlack(db, api, historyCollection)
	updateChannelsFromSlack(db, api, historyCollection)
}