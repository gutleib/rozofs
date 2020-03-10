

typedef struct _rozofs_bt_io_entry_t
{
   uin64_t next_off;  /**< next offset to read   */
   int     status;    /**< io status             */
   int     errcode;   /**< errno when status is negative */
   uint8_t pending_request;  /**< number of pending request for that context */
   uint8_t cmd_idx;          /**< index within the batch IO request          */
} rozofs_bt_io_entry_t;


typedef struct _rozofs_bt_ioctx_t
{
   void *io_batch_p;   /**< pointer to the io batch command (payload of ruc_buffer */
   void *io_ruc_buffer; /**< pointer to the ruc_buffer that contains the commands */
   int  socket;         /**< reference of the client socket                       */
   uint8_t cur_cmd_idx; /**< index of the last command for which a request has been submitted */
   rozofs_bt_io_entry_t io_entry[ROZOFS_MAX_BATCH_CMD];
}
