package main

import (
	"os"
	"github.com/jasonlvhit/gocron"
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
	var chat map[string]string
	// iterate through each group/channel's ID in MongoDB
	iter := chatCollection.Find(nil).Select(bson.M{"_id":1, "name":1}).Iter()
	for iter.Next(&chat){
		cId := chat["_id"]
		cName := chat["name"]
		messageHistory := new(History)
		log.Println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
		log.Println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
		log.Println("~~~~~~~~~ Slack History for " + cName + "(" + cId + ") ~~~~~~~~~")
		// find current group/channel's history record in the collection
		err :=	historyCollection.Find(bson.M{"_id":cId}).One(&messageHistory)
		if err != nil {
			// if there is not record for this group/channel yet, initialize the history to read from the start of it's slack history
			if err == mgo.ErrNotFound {
				messageHistory.Id = cId
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
		// get the slack conversations tied to the group/channel, continue to run in loop
		// while hasMore variable indicates more slack history objects to query and save
		for hasMore {
			params := slack.HistoryParameters{Count: 100, Latest: latestTime, Oldest: oldestTime}
			history, _ := getHistory(cId, params)
			jsonHistory, _ := json.Marshal(history)
			log.Print("--- [" + time.Now().String() + "] History from Slack\r\n"+string(jsonHistory))

			// Add to Messages to collection
			info, err := historyCollection.UpsertId(cId,
				bson.M{"$addToSet": bson.M{"Messages": bson.M{"$each": history.Messages } } })
			if err != nil {
				panic(err)
			}
			jsonInfo, _ := json.Marshal(info)
			log.Print("--- [" + time.Now().String() + "] Added Messages to history collection\r\n"+string(jsonInfo))

			// if this is the first iteration in the for loop assign the first message's
			// timestamp as the new Latest value to save for the current group's history
			if isFirstRun {
				var ts string
				if len(history.Messages) > 0 {
					ts = history.Messages[0].Timestamp
					info, err := historyCollection.UpsertId(cId,
						bson.M{"$set" : bson.M{"latest" : ts}})
					if err != nil {
						panic(err)
					}
					jsonInfo, _ := json.Marshal(info)
					log.Print("--- [" + time.Now().String() + "] Updated Latest Timestamp\r\n"+string(jsonInfo))
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
	log.Println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
	log.Println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
	log.Println("~~~~~~~~~~~~~~~ Updating Slack Groups  ~~~~~~~~~~~~~~~~~~~")
	// loop through current list and update the collection in MongoDB
	for _, group := range groups {
		groupJson, err := json.Marshal(&group)
		var groupData map[string]interface{}
		if err := json.Unmarshal(groupJson, &groupData); err != nil {
			panic(err)
		}

		log.Println("---- [" + time.Now().String() + "] Slack Group : " + group.Name + "(" + group.ID + ")")
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
	log.Println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
	log.Println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
	log.Println("~~~~~~~~~~~~~~ Updating Slack Channels  ~~~~~~~~~~~~~~~~~~")
	// loop through current list and update the collection in MongoDB
	for _, channel := range channels {
		channelJson, err := json.Marshal(&channel)
		var channelData map[string]interface{}
		if err := json.Unmarshal(channelJson, &channelData); err != nil {
			panic(err)
		}

		log.Println("---- [" + time.Now().String() + "] Slack Channel : " + channel.Name + "(" + channel.ID + ")")
		_, err = channelsCollection.Upsert(bson.M{"_id": channel.ID}, channelData)
		if err != nil {
			panic(err)
		}
	}
	addMessagesFromSlackHistory(historyCollection, channelsCollection, api.GetChannelHistory)
}

func updateUsersFromSlack(db *mgo.Database, api *slack.Client) {
	usersCollection := db.C("users")
	// get latest list of channels from api
	users, err := api.GetUsers()
	if err != nil {
		panic(err)
	}
	log.Println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
	log.Println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
	log.Println("~~~~~~~~~~~~~~ Updating Slack Users  ~~~~~~~~~~~~~~~~~~")
	// loop through current list and update the collection in MongoDB
	for _, user := range users {
		userJson, err := json.Marshal(&user)
		var userData map[string]interface{}
		if err := json.Unmarshal(userJson, &userData); err != nil {
			panic(err)
		}

		log.Println("---- [" + time.Now().String() + "] Slack User : " + user.Name + "(" + user.ID + ")")
		_, err = usersCollection.Upsert(bson.M{"_id": user.ID}, userData)
		if err != nil {
			panic(err)
		}
	}
}

func updateAllFromSlack(db *mgo.Database, api *slack.Client, historyCollection *mgo.Collection) {
	updateUsersFromSlack(db, api)
	updateGroupsFromSlack(db, api, historyCollection)
	updateChannelsFromSlack(db, api, historyCollection)
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

	s := gocron.NewScheduler()
	s.Every(1).Day().At(os.Getenv("CRON_TIME")).Do(updateAllFromSlack, db, api, historyCollection)
	<- s.Start()
}