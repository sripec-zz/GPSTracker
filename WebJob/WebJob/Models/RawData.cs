using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WebJob.Models
{
    class RawData
    {
        public long Id { get; set; }
        public DateTime EventUTC { get; set; }
        public string DeviceId { get; set; }
        public double Lat { get; set; }
        public double Lon { get; set; }
    }
}
