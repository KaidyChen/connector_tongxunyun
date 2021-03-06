�}.  5   �Ĝ�?�a��u
��	ccP=+a��D@��X6��,��@ �5NoJ��fr���R�-Q#��^�W���8��i�]���6�m�;)< ?���!����B:�);�2یL�
�1'`��G���j�/ �"�\M�(D׸���W(�l��˼��#E��7y3�����?�.�ɶ��A��%,yX �)Š�U͖��3iX���JE[WT�4��	*_�8�H+<e���:��]����J��w����;��{F#��=-\�4�e�9Y��k��2ʒ~a�l�c��x�l��}���"ѝ���q�U��G&�2��і��<#����_��U,�v;����	K�5�~���H�V�:������zX�ҹ1>;�`�s%�4��h�0�l�R�`L^X�^9�/>|%�	���U2��"u�ٲ��<��K;y��l*eW�֌�N�L�D�.�]	�L͢�0��D�=�ˣ?� ���>J��d���tatus, Time) ->
    ets:insert(?TABLE_ID, {GatewayCode, Status, Time}),
    ok.

lookup(GatewayCode) ->
    case ets:lookup(?TABLE_ID, GatewayCode) of
        [{GatewayCode, Status, Time}] ->
            ok;
        [] ->
            {error, not_found}    
    end.

delete(GatewayCode) ->
    ets:delete(?TABLE_ID, GatewayCode).

show() ->
    case ets:tab2list(?TABLE_ID) of
        Info ->
            {ok, Info};
        [] ->           
            {error, no_data}
    end.
