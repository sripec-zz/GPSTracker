using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using System.Web.Mvc;
using Dapper;
using Microsoft.ServiceBus.Messaging;
using Webmap.Models;

namespace Webmap.Controllers
{
    public class HomeController : Controller
    {
        string ConnectionString = ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString;

        public ActionResult Index()
        {

            var location = new Location() { Lat = 13.095683, Lon = 80.261197 };

            return View(location);
        }

        public ActionResult Location(string date)
        {
            var dateNow = string.IsNullOrEmpty(date) ? DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss") : date;

            var lstLocation = new List<RootObject>();
            using (var sqlConnection = new SqlConnection(ConnectionString))
            {
                var locationData = sqlConnection.Query<dynamic>(string.Format("select * from RawData where EventUtc>='{0}' order by EventUTC asc", dateNow)).ToList();
                if (locationData != null && locationData.Count > 0)
                {
                    foreach (var item in locationData)
                    {
                        var location = new RootObject();
                        location.Id = item.Id;
                        location.PlaceName = "Chennai";
                        location.GeoLong = item.Lat.ToString();
                        location.GeoLat = item.Lon.ToString();
                        location.EventUtc = item.EventUTC.ToString("yyyy-MM-dd HH:mm:ss");
                        lstLocation.Add(location);
                    }
                }
                else
                {
                    var locationDataLatest = sqlConnection.Query<dynamic>(string.Format("select * from RawData where EventUtc<'{0}' order by EventUTC desc", dateNow)).FirstOrDefault();
                    if(locationDataLatest != null)
                    {
                        var location = new RootObject();
                        location.Id = locationDataLatest.Id;
                        location.PlaceName = "Chennai";
                        location.GeoLong = locationDataLatest.Lat.ToString();
                        location.GeoLat = locationDataLatest.Lon.ToString();
                        location.EventUtc = locationDataLatest.EventUTC.ToString("yyyy-MM-dd HH:mm:ss");
                        lstLocation.Add(location);
                    }
                }
            }

            return Json(lstLocation, JsonRequestBehavior.AllowGet);

        }

        public ActionResult About()
        {
            ViewBag.Message = "Your application description page.";

            return View();
        }

        public ActionResult Contact()
        {
            ViewBag.Message = "Your contact page.";

            return View();
        }

        public class RootObject
        {
            public long Id { get; set; }
            public string PlaceName { get; set; }
            public string GeoLong { get; set; }
            public string GeoLat { get; set; }

            public string EventUtc { get; set; }
        }
    }
}