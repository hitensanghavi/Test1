using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.ComponentModel.DataAnnotations;
using Microsoft.EntityFrameworkCore;
using testWebApplication1.Models;
using System.ComponentModel.DataAnnotations.Schema;

namespace testWebApplication1.Models 
{
    /// <summary>
    /// File Model contains attributes for any file that is uploaded. File is stored as Blob in Azure with URL/address stored here. 
    /// If there is an image file 3 sizes of image may be stored to optimize the display - Original, small and Medium. The address for these will be stored in the various URL fields.
    /// Will support any document format (.doc, .xlsx, ....) and image format (JPEG, PNG...)
    /// Will support various video formats (?????)
    /// </summary>
    public class File
    {

        [Required]
        public int ID { get; set; }

        //Name of the file 
        [Required]
        public string FileName { get; set; }

        //Address for the original file
        [Required]
        public String OriginalFileURL { get; set; }

        // Address for thumbnail image. Size (???)
        public String ThumbFileURL { get; set; }

        // Address for Medium size image. Size (???)
        public String MediumFileURL { get; set; }
    }
}
