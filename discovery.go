package main

import (
	"database/sql"
	"flag"
	"fmt"
	lastfm "github.com/turnerd18/go-lastfm"
	"math"
	"os"
	"strconv"
	"time"
	_ "github.com/go-sql-driver/mysql"
)

func main() {
	flag.Parse()

	// get all users
	type User struct {
		Name string
		Sk string
	}
	con, err := sql.Open("mysql", dbstring)
	if err != nil {
		con.Close()
		os.Exit(1)
	}
	rows, err := con.Query("SELECT user, sk FROM users")
	if err != nil {
		fmt.Println("error selecting: " + err.Error())
		os.Exit(1)
	}
	var users []User
	var user User
	for rows.Next() {
		rows.Scan(&user.Name, &user.Sk)
		users = append(users, user)
	}

	type MyTrack struct {
		Track lastfm.APITrack
		Playcount string
	}
	// get all scrobbles for each user
	for _, user = range users {
		api, err := lastfm.NewAPI(apikey, apisecret, user.Name, user.Sk)
		if err != nil {
			fmt.Println("error making new api")
			os.Exit(1)
		}
		// get user playcount to calculate number of pages to request
		uinfo, err := api.UserGetInfo(user.Name)
		if err != nil {
			fmt.Println("error getting user playcount: ", err.Error())
			os.Exit(1)
		}
		playcount, _ := strconv.ParseFloat(uinfo.Playcount, 32)
		pagelimit := int(math.Ceil(playcount / 200) + 1)
		from, _ := time.Parse(time.RFC822, "01 Jan 01 00:00 CST")
		to := time.Now()
		var tracks []lastfm.APITrack
		// get all user scrobbles
		for page := 1; page < pagelimit; page++ {
			t, err := api.UserGetRecentTracks(user.Name, 200, page, from.Unix(), to.Unix())
			if err != nil {
				fmt.Println("error getting recent tracks at page ", page, " ", err.Error())
			} else {
				tracks = append(tracks, t...)
			}
		}
		// make map of first time each track was played
		// assume last instance of track in tracks is oldest
		firstplays := make(map[string]*MyTrack)
		for _, t := range tracks {
			insert := MyTrack{}
			insert.Track = t
			firstplays[t.Name + "|" + t.Artist.Text + "|" + t.Album.Text] = &insert
		}
		// get user library for playcounts
		pagelimit = len(firstplays) / 10
		for page := 1; page < 12; page++ {
			library, err := api.LibraryGetTracks(user.Name, "", "", page, pagelimit)
			if err != nil {
				fmt.Println("error getting library tracks at page ", page, " ", err.Error())
			} else {
				for _, t := range library {
					if firstplays[t.Name + "|" + t.Artist.Name + "|" + t.Album.Name] != nil {
						firstplays[t.Name + "|" + t.Artist.Name + "|" + t.Album.Name].Playcount = t.Playcount
					}
				}
			}
		}
		// build sql insert query
		query := "INSERT INTO song_discovery (name, artist, album, date, user, playcount, image) VALUES "
		for _, t := range firstplays {
			timestamp, _ := strconv.ParseInt(t.Track.Date.Uts, 10, 64)
			playcount, _ := strconv.ParseInt(t.Playcount, 10, 16)
			query += fmt.Sprintf("(%q, %q, %q, %d, %q, %d, %q), ", t.Track.Name, t.Track.Artist.Text, t.Track.Album.Text, timestamp, user.Name, playcount, t.Track.Image[2].Text)
		}
		query = query[:len(query)-2] + " ON DUPLICATE KEY UPDATE date=VALUES(date), playcount=VALUES(playcount), image=VALUES(image);"
		// insert/update tracks in database
		_, err = con.Exec(query)
		if err != nil {
			fmt.Println("error inserting scrobbles record: " + err.Error())
			os.Exit(1)
		}
	}
	con.Close()
}
