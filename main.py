import json
import mysql.connector
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, jsonify, make_response, send_file, send_from_directory
from flask_cors import CORS
import glob
import os
import base64
import gzip

app = Flask(__name__)
CORS(app)  # prevents the CORS response header error in browser


def fetch_data():
    try:
        con = mysql.connector.connect(user='root', password='DdlrsIjzp52YeOs8',
                                      host='localhost', database="improvethenews", port="9001")
    except mysql.connector.Error as err:
        print(err)
    else:
        cursor = con.cursor()

        # region Media info
        cursor.execute(
            "SELECT label, name, lr, pe FROM `media` "
        )
        media_infos = cursor.fetchall()
        media_arr = []
        for media_info in media_infos:
            (media_label, media_name, media_pbias, media_ebias) = media_info
            media_arr.append({
                "label": media_label,
                "name": media_name,
                "politicalBias": media_pbias / 2 - 1.5,
                "establishmentBias": media_ebias / 2 - 1.5,
            })
        # print(media_arr)
        # endregion

        topic_jsons = []
        topic_colours = ["0x61C2FA", "0xB95B99", "0x2F4DF5", "0x71F2CF", "0x70257A", "0x054A64", "0xD64C38", "0x26D0E6",
                         "0x825D70", "0x51B088", "0xF1A14A", "0xEA465E", "0xFF693D", "0x1E77D7"]
        topics_dict = dict()

        # region Construct first-order topics
        first_order_topics_dict = dict()  # maps first order topics to their names

        cursor.execute(
            "SELECT id, ancestorlist, label, name FROM `topics` "
        )

        topics = cursor.fetchall()

        topic_index = 0
        for topic in topics:
            (topic_id, topic_ancestor, topic_label, topic_name) = topic
            ancestors = [int(x) for x in topic_ancestor.split(',') if x.strip().isdigit()]
            if len(ancestors) == 1:
                # topic is "first order", i.e. single ancestor
                # create first order topic mappings
                first_order_topics_dict[topic_id] = topic_label
                topic_jsons.append({
                    "id": topic_id,
                    "label": topic_label,
                    "name": topic_name,
                    "stories": [],
                    "numArticles": 0,
                    "color": topic_colours[topic_index] if topic_index < len(topic_colours) else "0x61C2FA",
                    "index": topic_index
                })
                topics_dict[topic_id] = topic_id
                topic_index += 1
            elif len(ancestors) > 1:
                topics_dict[topic_id] = ancestors[1]
            else:
                # this should only happen in the case of the root of the tree i.e. "news" node
                # that everything else falls under
                topics_dict[topic_id] = topic_id

        def find_first_order_topic_id(_topic_id: int) -> int:
            if _topic_id in topics_dict.keys():
                return topics_dict[_topic_id]
            else:
                raise Exception("topic id not found in topic dict! check code")
        # endregion

        # region Fetch start date
        # this should fetch the latest cluster update
        cursor.execute(
            "SELECT enddate FROM `clusters` "
            "ORDER BY enddate DESC "
            "LIMIT 1 "
        )
        db_latest_article: datetime = cursor.fetchone()[0]
        # endregion

        # region Fetch article data
        def add_to_topic(_story_id: int, _topic_label: str, _num_articles: int) -> None:
            for topic_json in topic_jsons:
                if topic_json["label"] == _topic_label:
                    if _story_id not in topic_json["stories"]:
                        topic_json["stories"].append(_story_id)
                    topic_json["numArticles"] += _num_articles
                    return

        # get all palantir_data from past month
        min_end_date = (db_latest_article - timedelta(weeks=5)).strftime("%Y-%m-%d")
        max_start_date = (db_latest_article + timedelta(days=1)).strftime("%Y-%m-%d")
        print(min_end_date)
        print(max_start_date)

        latest_article = datetime.min

        # SQL injection threat!!! don't allow publicly manipulated variables for any string concats
        # https://dev.mysql.com/doc/connector-python/en/connector-python-api-mysqlcursor-execute.html
        cursor.execute(
            "SELECT c.id, c.title, c.startdate, c.enddate, a.aID, a.id, a.utcdate, a.title, a.url, m.label, t.topic_id,"
            " si.image "
            "FROM ( "
            "   SELECT * FROM clusters "
            "   WHERE size2 >= 10 "
            "   AND enddate IS NOT NULL "
            "   AND DATE(enddate) >= \"" + min_end_date + "\" "
            "   AND startdate IS NOT NULL "
            "   AND DATE(startdate) <= \"" + max_start_date + "\""
            "   ORDER BY timestamp ASC "
            # "   LIMIT 1 "
            ") c "
            "INNER JOIN clustermatches cm ON cm.cluster_id = c.id "
            "INNER JOIN articles a ON a.aID = cm.aid "
            "INNER JOIN media m ON m.label = a.medianame "
            "INNER JOIN topictags t ON t.aid = a.aid "
            "LEFT JOIN storyimages si on si.storyid = cm.cluster_id "
            "WHERE cm.status = 2 "
        )
        clusters = cursor.fetchall()
        story_jsons = []
        # maps cluster_ids to list of topic names
        stories_to_json = dict()  # number -> list[string]

        article_jsons = []
        # maps article "aid"s to list of topic names
        articles_to_json = dict()  # number -> list[string]
        previous_cluster = None

        for cluster in clusters:
            (cluster_id, cluster_name, cluster_start, cluster_end, article_aid, article_id, article_utcdate,
             article_title, article_url, media_label, topic_id, story_image) = cluster

            # if story_image is not None:
            #     print(type(story_image))

            if previous_cluster is None:
                # only runs on first iteration
                article_jsons = []
                articles_to_json = dict()
            elif previous_cluster[0] != cluster_id:
                # when the cluster changes
                # write the previous cluster information to story_jsons
                for k, v in articles_to_json.items():
                    article_jsons.append(v)

                story_jsons.append({
                    "id": previous_cluster[0],
                    "name": previous_cluster[1],
                    "starttime": previous_cluster[2].isoformat(),
                    "endtime": previous_cluster[3].isoformat(),
                    "topics": stories_to_json[previous_cluster[0]],
                    "story_image": base64.b64encode(previous_cluster[11]).decode() if previous_cluster[11] is not None else None,
                    "articles": article_jsons
                })

                article_jsons = []
                articles_to_json = dict()

            first_order_id = find_first_order_topic_id(topic_id)
            first_order_name = first_order_topics_dict.get(first_order_id)
            add_to_topic(cluster_id, first_order_name, 1)

            if article_aid not in articles_to_json:
                latest_article = max(latest_article, article_utcdate)
                articles_to_json[article_aid] = {
                    "id": article_id,
                    "topics": [first_order_name],
                    "story": cluster_name,
                    "title": article_title,
                    "link": article_url,
                    "time": article_utcdate.isoformat(),
                    "media_label": media_label,
                }
            elif first_order_name not in articles_to_json[article_aid]["topics"]:
                articles_to_json[article_aid]["topics"].append(first_order_name)

            if cluster_id not in stories_to_json:
                stories_to_json[cluster_id] = [first_order_name]
            elif first_order_name not in stories_to_json[cluster_id]:
                stories_to_json[cluster_id].append(first_order_name)

            previous_cluster = cluster

        for k, v in articles_to_json.items():
            article_jsons.append(v)

        if previous_cluster is not None:
            story_jsons.append({
                "id": previous_cluster[0],
                "name": previous_cluster[1],
                "starttime": previous_cluster[2].isoformat(),
                "endtime": previous_cluster[3].isoformat(),
                "topics": stories_to_json[previous_cluster[0]],
                "story_image": base64.b64encode(story_image).decode() if story_image is not None else None,
                "articles": article_jsons
            })

            final_json = {
                "timestamp": datetime.now().isoformat(),
                "latest_article": latest_article.isoformat(),
                "media_info": media_arr,
                "topics": topic_jsons,
                "stories": story_jsons
            }

            with gzip.open("./output/monthly_" + datetime.now().strftime("%Y_%m_%d-%H_%M") + ".json.gz", "wt") as f:
                f.write(json.dumps(final_json, default=str))
        # endregion

        con.close()


# Flask API portion
@app.route("/")
def index():
    # get most recent daily and monthly info
    list_of_monthly_files = glob.glob("./output/*.json.gz")
    latest_monthly_file = max(list_of_monthly_files, key=os.path.getctime)
    print(open(latest_monthly_file))
    # monthly_data = json.load(open(latest_monthly_file))
    # response = make_response()
    # return jsonify(monthly_data)
    return send_file(latest_monthly_file)


if __name__ == "__main__":
    scheduler = BackgroundScheduler()
    scheduler.add_job(func=lambda: fetch_data(), trigger="interval", hours=4)
    scheduler.start()

    # uncomment to force fetch right now
    # fetch_data()

    app.run()
